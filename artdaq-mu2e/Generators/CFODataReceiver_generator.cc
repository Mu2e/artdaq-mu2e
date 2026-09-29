// This file reads out DTCs that are NOT in HW Event-building mode
// It can be used as an exmaple for developing more specific functionality.

#include "artdaq-core-mu2e/Overlays/FragmentType.hh"
#include "dtcInterfaceLib/DTC.h"
#include "cfoInterfaceLib/CFO.h"
#include "dtcInterfaceLib/DTCSoftwareCFO.h"

#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Data/MetadataFragment.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Generators/GeneratorMacros.hh"

#include "artdaq-core-mu2e/Overlays/CFO_Packets/CFO_EventRecord.h"
#include "artdaq-core-mu2e/Overlays/DTC_Types/DTC_EventMode.h"

#include <libpq-fe.h>

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <iomanip>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>

#include "trace.h"
#define TRACE_NAME "CFODataReceiver"

namespace mu2e {
class CFODataReceiver : public artdaq::CommandableFragmentGenerator
{
public:
	explicit CFODataReceiver(fhicl::ParameterSet const& ps);
	virtual ~CFODataReceiver();

	DTCLib::DTC_SimMode GetMode() { return mode_; }

private:
	bool getNextDTCFragment(artdaq::FragmentPtrs& output, DTCLib::DTC_EventWindowTag ts);

	void start() override;

	void stopNoMutex() override {}

	void stop() override;

	size_t getCurrentSequenceID();

	// Like "getNext_", "fragmentIDs_" is a mandatory override; it
	// returns a vector of the fragment IDs an instance of this class
	// is responsible for (in the case of CFODataReceiver, this is just
	// the fragment_id_ variable declared in the parent
	// CommandableFragmentGenerator class)

	std::vector<artdaq::Fragment::fragment_id_t> fragmentIDs_() { return fragment_ids_; }

	std::vector<artdaq::Fragment::fragment_id_t> fragment_ids_;

	// State
	DTCLib::DTC_SimMode mode_;  //!=0 is simulation mode
	const bool skip_cfo_init_;
	bool print_packets_;

	size_t first_timestamp_seen_{size_t(-1)}, last_fragment_timestamp_{size_t(-1)};

	std::unique_ptr<CFOLib::CFO> theCFO_;
	//        std::unique_ptr<DTCLib::DTCSoftwareCFO> theCFO_;

	std::size_t const throttle_usecs_;
	std::size_t rollover_subrun_interval_;
	std::condition_variable throttle_cv_;
	std::mutex throttle_mutex_;
	int diagLevel_;
	std::map<uint64_t, int> ewts_;  // for checking if an event window tag has been found before

	// Per-subrun record
	struct SubrunRecord
	{
		int subrun_number{-1};
		uint64_t n_events{0};
		uint64_t n_on_spill{0};
		uint64_t n_off_spill{0};
		uint64_t n_null{0};
		uint64_t min_ewt{uint64_t(-1)};
		uint64_t max_ewt{0};
		uint32_t start_time_unix{0};                     // linux_timestamp of first event
		uint32_t stop_time_unix{0};                      // linux_timestamp of last event
		std::map<uint64_t, uint64_t> event_mode_counts;  // raw event_mode value -> count

		void reset(int subrun)
		{
			subrun_number = subrun;
			n_events = 0;
			n_on_spill = 0;
			n_off_spill = 0;
			n_null = 0;
			min_ewt = uint64_t(-1);
			max_ewt = 0;
			start_time_unix = 0;
			stop_time_unix = 0;
			event_mode_counts.clear();
		}
	};

	SubrunRecord subrun_record_;

	// File output
	std::string subrun_record_dir_;
	std::ofstream subrun_record_file_;

	void openRecordFile_();
	void writeRecordToFile_(const SubrunRecord& rec);

	// DB output — background writer thread
	std::string subrun_record_db_connstr_;
	std::string db_schema_;

	static constexpr size_t kDbQueueMaxSize = 20;
	std::queue<SubrunRecord> db_queue_;
	std::mutex db_queue_mutex_;
	std::condition_variable db_queue_cv_;
	std::atomic<bool> db_writer_stop_{false};
	std::thread db_writer_thread_;

	void dbWriterThread_();
	void writeRecordToDb_(PGconn* conn, const SubrunRecord& rec);

	// Push a copy of the current record to both outputs (called at subrun boundary)
	void publishRecord_();

	// The "getNext_" function is used to implement user-specific
	// functionality; it's a mandatory override of the pure virtual
	// getNext_ function declared in CommandableFragmentGenerator

	bool getNext_(artdaq::FragmentPtrs& output) override;
	DTCLib::DTC_EventWindowTag getCurrentEventWindowTag();
};
}  // namespace mu2e

// ---------------------------------------------------------------------------
// Helpers: build event_mode JSON string from a SubrunRecord
// ---------------------------------------------------------------------------

static std::string buildEventModeJson(const std::map<uint64_t, uint64_t>& counts)
{
	std::ostringstream oss;
	oss << "{";
	bool first = true;
	for (auto const& kv : counts)
	{
		if (!first) oss << ",";
		oss << "\"" << kv.first << "\":" << kv.second;
		first = false;
	}
	oss << "}";
	return oss.str();
}

// ---------------------------------------------------------------------------
// File output
// ---------------------------------------------------------------------------

void mu2e::CFODataReceiver::openRecordFile_()
{
	if (subrun_record_dir_.empty()) return;

	std::ostringstream fname;
	fname << subrun_record_dir_;
	if (subrun_record_dir_.back() != '/') fname << '/';
	fname << "subrun_record_run" << std::setw(6) << std::setfill('0') << run_number() << ".csv";

	subrun_record_file_.open(fname.str(), std::ios::app);
	if (!subrun_record_file_.is_open())
	{
		TLOG(TLVL_WARNING) << "Could not open subrun record file: " << fname.str();
		return;
	}

	subrun_record_file_.seekp(0, std::ios::end);
	if (subrun_record_file_.tellp() == 0)
	{
		subrun_record_file_
			<< "run_number,subrun_number"
			<< ",n_events,n_on_spill,n_off_spill,n_null"
			<< ",min_ewt,max_ewt"
			<< ",start_time_unix,stop_time_unix"
			<< ",event_mode_counts_json"
			<< "\n";
	}
	TLOG(TLVL_DEBUG) << "Opened subrun record file: " << fname.str();
}

void mu2e::CFODataReceiver::writeRecordToFile_(const SubrunRecord& rec)
{
	if (!subrun_record_file_.is_open()) return;
	if (rec.n_events == 0) return;

	subrun_record_file_
		<< run_number()
		<< "," << rec.subrun_number
		<< "," << rec.n_events
		<< "," << rec.n_on_spill
		<< "," << rec.n_off_spill
		<< "," << rec.n_null
		<< "," << rec.min_ewt
		<< "," << rec.max_ewt
		<< "," << rec.start_time_unix
		<< "," << rec.stop_time_unix
		<< ",\"" << buildEventModeJson(rec.event_mode_counts) << "\""
		<< "\n";
	subrun_record_file_.flush();

	TLOG(TLVL_DEBUG) << "Wrote file record for subrun " << rec.subrun_number
					 << ": " << rec.n_events << " events"
					 << " (" << rec.n_on_spill << " on-spill, " << rec.n_off_spill << " off-spill)";
}

// ---------------------------------------------------------------------------
// DB output — background writer thread
// ---------------------------------------------------------------------------

void mu2e::CFODataReceiver::writeRecordToDb_(PGconn* conn, const SubrunRecord& rec)
{
	if (rec.n_events == 0) return;

	const std::string mode_json = buildEventModeJson(rec.event_mode_counts);

	// Use parameterized query to avoid any injection issues
	const std::string sql =
		"INSERT INTO " + db_schema_ + ".subrun "
		"(run_number, subrun_number, n_events, n_on_spill, n_off_spill, n_null, "
		" min_ewt, max_ewt, start_time_unix, stop_time_unix, event_mode_counts) "
		"VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) "
		"ON CONFLICT (run_number, subrun_number) DO UPDATE SET "
		"  n_events=EXCLUDED.n_events, n_on_spill=EXCLUDED.n_on_spill, "
		"  n_off_spill=EXCLUDED.n_off_spill, n_null=EXCLUDED.n_null, "
		"  min_ewt=EXCLUDED.min_ewt, max_ewt=EXCLUDED.max_ewt, "
		"  start_time_unix=EXCLUDED.start_time_unix, "
		"  stop_time_unix=EXCLUDED.stop_time_unix, "
		"  event_mode_counts=EXCLUDED.event_mode_counts";

	const std::string p1 = std::to_string(run_number());
	const std::string p2 = std::to_string(rec.subrun_number);
	const std::string p3 = std::to_string(rec.n_events);
	const std::string p4 = std::to_string(rec.n_on_spill);
	const std::string p5 = std::to_string(rec.n_off_spill);
	const std::string p6 = std::to_string(rec.n_null);
	const std::string p7 = std::to_string(rec.min_ewt);
	const std::string p8 = std::to_string(rec.max_ewt);
	const std::string p9 = std::to_string(rec.start_time_unix);
	const std::string p10 = std::to_string(rec.stop_time_unix);
	const std::string p11 = mode_json;

	const char* params[11] = {p1.c_str(), p2.c_str(), p3.c_str(), p4.c_str(), p5.c_str(),
							  p6.c_str(), p7.c_str(), p8.c_str(), p9.c_str(), p10.c_str(),
							  p11.c_str()};

	PGresult* res = PQexecParams(conn, sql.c_str(), 11, nullptr, params, nullptr, nullptr, 0);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		TLOG(TLVL_WARNING) << "DB insert failed for subrun " << rec.subrun_number
						   << ": " << PQresultErrorMessage(res);
	else
		TLOG(TLVL_DEBUG) << "DB record written for subrun " << rec.subrun_number;

	PQclear(res);
}

void mu2e::CFODataReceiver::dbWriterThread_()
{
	if (subrun_record_db_connstr_.empty()) return;

	PGconn* conn = PQconnectdb(subrun_record_db_connstr_.c_str());
	if (PQstatus(conn) != CONNECTION_OK)
	{
		TLOG(TLVL_WARNING) << "DB connection failed: " << PQerrorMessage(conn)
						   << " — subrun records will not be written to DB";
		PQfinish(conn);
		// Drain the queue without writing so pushes don't pile up
		while (true)
		{
			std::unique_lock<std::mutex> lock(db_queue_mutex_);
			db_queue_cv_.wait(lock, [&] { return !db_queue_.empty() || db_writer_stop_.load(); });
			while (!db_queue_.empty()) db_queue_.pop();
			if (db_writer_stop_.load()) break;
		}
		return;
	}

	TLOG(TLVL_DEBUG) << "DB writer thread connected";

	while (true)
	{
		SubrunRecord rec;
		{
			std::unique_lock<std::mutex> lock(db_queue_mutex_);
			db_queue_cv_.wait(lock, [&] { return !db_queue_.empty() || db_writer_stop_.load(); });

			if (db_queue_.empty()) break;  // stop_ set and queue drained

			rec = std::move(db_queue_.front());
			db_queue_.pop();
		}

		// Reconnect if connection was lost
		if (PQstatus(conn) != CONNECTION_OK)
		{
			PQreset(conn);
			if (PQstatus(conn) != CONNECTION_OK)
			{
				TLOG(TLVL_WARNING) << "DB reconnect failed, dropping subrun " << rec.subrun_number;
				continue;
			}
		}

		writeRecordToDb_(conn, rec);
	}

	PQfinish(conn);
	TLOG(TLVL_DEBUG) << "DB writer thread exiting";
}

// ---------------------------------------------------------------------------
// Publish: called at subrun boundary — copies record to file and DB queue
// ---------------------------------------------------------------------------

void mu2e::CFODataReceiver::publishRecord_()
{
	if (subrun_record_.n_events == 0) return;

	writeRecordToFile_(subrun_record_);

	if (!subrun_record_db_connstr_.empty())
	{
		std::unique_lock<std::mutex> lock(db_queue_mutex_);
		if (db_queue_.size() >= kDbQueueMaxSize)
		{
			TLOG(TLVL_WARNING) << "DB queue full, dropping subrun record " << subrun_record_.subrun_number;
		}
		else
		{
			db_queue_.push(subrun_record_);
			db_queue_cv_.notify_one();
		}
	}
}

// ---------------------------------------------------------------------------

mu2e::CFODataReceiver::~CFODataReceiver()
{
}

bool mu2e::CFODataReceiver::getNext_(artdaq::FragmentPtrs& frags)
{
	TLOG(TLVL_DEBUG + 30) << "getNext_";
	// while (!should_stop())
	// {
	//      TLOG(TLVL_DEBUG + 31) << "Sleeping...";
	//      usleep(5000);
	// }

	if (throttle_usecs_ > 0)
	{
		TLOG(TLVL_DEBUG + 32) << "Throttling... " << throttle_usecs_;
		std::unique_lock<std::mutex> throttle_lock(throttle_mutex_);
		throttle_cv_.wait_for(throttle_lock, std::chrono::microseconds(throttle_usecs_), [&]() { return should_stop(); });
	}

	if (should_stop())
	{
		TLOG(TLVL_DEBUG + 33) << "Stopping.";
		return false;
	}

	uint64_t z = 0;
	DTCLib::DTC_EventWindowTag zero(z);

	TLOG(TLVL_DEBUG + 34) << "getNext_ req";
	auto start_time = std::chrono::steady_clock::now();
	bool retVal = true;
	do
	{
		retVal = getNextDTCFragment(frags, zero);
		TLOG(TLVL_DEBUG + 35) << "getNext_ req retry? " << retVal << " " << frags.size();
	} while (1 && retVal && frags.size() < 900 &&
			 artdaq::TimeUtils::GetElapsedTimeMicroseconds(start_time) < 100000 /* 100 ms */);
	TLOG(TLVL_DEBUG + 36) << "getNext_ req done" << retVal << " " << frags.size();

	return retVal;
}  // end getNext_()

DTCLib::DTC_EventWindowTag mu2e::CFODataReceiver::getCurrentEventWindowTag()
{
	if (first_timestamp_seen_ != size_t(-1))
	{
		return DTCLib::DTC_EventWindowTag(getCurrentSequenceID() + first_timestamp_seen_);
	}

	return DTCLib::DTC_EventWindowTag(uint64_t(0));
}

mu2e::CFODataReceiver::CFODataReceiver(fhicl::ParameterSet const& ps)
	: CommandableFragmentGenerator(ps)
	, fragment_ids_{static_cast<artdaq::Fragment::fragment_id_t>(fragment_id())}
	, mode_(DTCLib::DTC_SimModeConverter::ConvertToSimMode(ps.get<std::string>("sim_mode", "Disabled")))
	, skip_cfo_init_(ps.get<bool>("skip_cfo_init", false))
	, print_packets_(ps.get<bool>("debug_print", false))
	, throttle_usecs_(ps.get<size_t>("throttle_usecs", 0))  // in units of us
	, rollover_subrun_interval_(ps.get<size_t>("rollover_subrun_interval", 20000))
	, diagLevel_(ps.get<int>("diagLevel", 0))
	, subrun_record_dir_(ps.get<std::string>("subrun_record_dir", ""))
	, subrun_record_db_connstr_(ps.get<std::string>("subrun_record_db_connstr", ""))
{
	// If no explicit connstr, try to build one from environment variables
	if (subrun_record_db_connstr_.empty())
	{
		const char* host = std::getenv("OTSDAQ_RUNINFO_DATABASE_HOST");
		const char* db = std::getenv("OTSDAQ_RUNINFO_DATABASE");
		const char* port = std::getenv("OTSDAQ_RUNINFO_DATABASE_PORT");
		const char* user = std::getenv("OTSDAQ_RUNINFO_DATABASE_USER");
		if (host && db && port && user)
		{
			std::ostringstream oss;
			oss << "host=" << host << " port=" << port
				<< " dbname=" << db << " user=" << user;
			subrun_record_db_connstr_ = oss.str();
			TLOG(TLVL_DEBUG) << "Built DB connstr from environment: " << subrun_record_db_connstr_;
		}
	}

	const char* schema_env = std::getenv("OTSDAQ_RUNINFO_DATABASE_SCHEMA");
	db_schema_ = schema_env ? schema_env : "online";

	// mode_ can still be overridden by environment!
	theCFO_ = std::make_unique<CFOLib::CFO>(mode_,
											ps.get<int>("cfo", -1),
											ps.get<std::string>("expectedDesignVersion", ""),
											skip_cfo_init_,
											ps.get<std::string>("uid", ""));

	mode_ = theCFO_->GetSimMode();
	TLOG(TLVL_DEBUG) << "CFODataReceiver Initialized with mode " << mode_;

	if (rollover_subrun_interval_ == 0)
	{
		TLOG(TLVL_WARNING) << "Subrun rollover is set to 0, overriding this with 1M";
		rollover_subrun_interval_ = 1e6;
	}

	if (skip_cfo_init_) return;  // skip any control of DTC

	theCFO_->ReleaseAllBuffers(DTC_DMA_Engine_DAQ);
}

void mu2e::CFODataReceiver::stop()
{
	publishRecord_();
	if (subrun_record_file_.is_open()) subrun_record_file_.close();

	// Signal DB writer and give it up to 3 seconds to flush
	if (db_writer_thread_.joinable())
	{
		db_writer_stop_.store(true);
		db_queue_cv_.notify_one();
		db_writer_thread_.join();  // already has a 3s deadline — see start()
	}

	ewts_.clear();
	// if (skip_cfo_init_) return;  // skip any control of DTC
}

void mu2e::CFODataReceiver::start()
{
	theCFO_->ReleaseAllBuffers(DTC_DMA_Engine_DAQ);
	ewts_.clear();

	subrun_record_.reset(subrun_number());
	metricMan->sendMetric("SubrunNumber", static_cast<uint64_t>(subrun_record_.subrun_number), "subrun", 1,
						  artdaq::MetricMode::LastPoint | artdaq::MetricMode::Persist);
	openRecordFile_();

	// Start DB writer thread if we have a connection string
	if (!subrun_record_db_connstr_.empty())
	{
		db_writer_stop_.store(false);
		db_writer_thread_ = std::thread([this]() {
			dbWriterThread_();
		});
	}
}

bool mu2e::CFODataReceiver::getNextDTCFragment(artdaq::FragmentPtrs& frags, DTCLib::DTC_EventWindowTag ts_in)
{
	const auto before_read = std::chrono::steady_clock::now();
	int retryCount = 5;
	std::vector<std::unique_ptr<CFOLib::CFO_Event>> data;
	while (data.size() == 0 && retryCount >= 0)
	{
		try
		{
			theCFO_->GetData(data, ts_in /* not used when not matching */);  // do we need to set matchEventWindowTag = true??
			TLOG(TLVL_DEBUG + 25) << "Done calling theCFO->GetData() data.size()=" << data.size() << ", retryCount=" << retryCount;
		}
		catch (std::exception const& ex)
		{
			TLOG(TLVL_ERROR) << "There was an error in the DTC Library: " << ex.what();
		}
		retryCount--;
	}
	if (retryCount < 0 && data.size() == 0)
	{
		// Return true if no data in external CFO mode, otherwise false
		return mode_ == 0;
	}
	const auto after_read = std::chrono::steady_clock::now();

	// GetSubEventData can return multiple EWTs, and we can assume that there is ONE DTC_SubEvent per EWT!
	// std::map<uint64_t, int> ewts; // for checking if an event window tag has been found before
	// std::map<uint64_t, std::unique_ptr<artdaq::ContainerFragmentLoader>> ewts; // for collecting fragments with the same event window tag
	for (auto& cfoevt : data)
	{
		const DTCLib::DTC_EventWindowTag ts_out = cfoevt->GetEventWindowTag();
		const auto fragment_timestamp = ts_out.GetEventWindowTag(true);
		TLOG(TLVL_DEBUG + 19) << "Received data with timestamp " << fragment_timestamp;
		if (ewts_.count(fragment_timestamp))
		{
			TLOG(TLVL_DEBUG + 17) << "Received data with timestamp " << fragment_timestamp << " " << ewts_[fragment_timestamp] << " times already";
			++ewts_[fragment_timestamp];
			// FIXME: Decide how to best handle these cases
			continue;
		}
		else
		{
			ewts_[fragment_timestamp] = 1;
			TLOG(TLVL_DEBUG + 18) << "Received data with timestamp " << fragment_timestamp << " for the first time";
		}
		TLOG(TLVL_DEBUG + 20) << "Initializing a CFO_Event ";
		auto evt = std::make_unique<CFOLib::CFO_Event>(&cfoevt);
		// TLOG(TLVL_DEBUG + 23) << "Setting Eventmode to " << (uint64_t) evt->GetEventMode();

		auto ptr = reinterpret_cast<const uint8_t*>(evt->GetRawBufferPointer());

		TLOG(TLVL_DEBUG + 21) << "Calling memcpy(" << (const void*)ptr << ", " << (void*)cfoevt->GetRawBufferPointer() << ", " << cfoevt->GetEventByteCount() << ")";
		memcpy(const_cast<uint8_t*>(ptr), cfoevt->GetRawBufferPointer(), cfoevt->GetEventByteCount());
		ptr += cfoevt->GetEventByteCount();

		TLOG(TLVL_DEBUG + 25) << "Creating Fragment, sz=" << evt->GetEventByteCount() << ", timestamp=" << fragment_timestamp;
		frags.emplace_back(new artdaq::Fragment(fragment_timestamp, fragment_ids_[0], FragmentType::CFO, fragment_timestamp));
		frags.back()->resizeBytes(evt->GetEventByteCount());
		memcpy(frags.back()->dataBegin(), evt->GetRawBufferPointer(), evt->GetEventByteCount());
		metricMan->sendMetric("Average Event Size", evt->GetEventByteCount(), "Bytes", 3, artdaq::MetricMode::Average);
		TLOG(TLVL_DEBUG + 26) << "Incrementing event counter";
		ev_counter_inc();

		// Per-subrun accounting
		{
			const CFOLib::CFO_EventRecord& rec = evt->GetEventRecord();
			const DTCLib::DTC_EventMode mode = evt->GetEventMode();

			if (rec.event_mode == 0)
			{
				++subrun_record_.n_null;
			}
			else
			{
				++subrun_record_.n_events;

				if (fragment_timestamp < subrun_record_.min_ewt) subrun_record_.min_ewt = fragment_timestamp;
				if (fragment_timestamp > subrun_record_.max_ewt) subrun_record_.max_ewt = fragment_timestamp;

				if (mode.isOnSpillFlagSet())
					++subrun_record_.n_on_spill;
				else
					++subrun_record_.n_off_spill;

				const uint32_t hw_time = static_cast<uint32_t>(rec.linux_timestamp);
				if (subrun_record_.start_time_unix == 0) subrun_record_.start_time_unix = hw_time;
				subrun_record_.stop_time_unix = hw_time;
			}

			++subrun_record_.event_mode_counts[rec.event_mode];
		}

		//--------------------------------------------------------------------------------
		// Sub-run transition: fire inline so the boundary is exact within the batch.
		// Triggered by hardware subrun bit OR software event-count interval.
		//--------------------------------------------------------------------------------
		const bool hw_subrun_trigger = (fragment_id() == 0) && evt->GetEventMode().isSubRunBitSet();
		const bool sw_subrun_trigger = (fragment_id() == 0) && (rollover_subrun_interval_ > 0) && (ev_counter() % rollover_subrun_interval_ == 0);
		if (hw_subrun_trigger || sw_subrun_trigger)
		{
			const auto next_subrun = subrun_record_.subrun_number + 1;
			TLOG(TLVL_DEBUG + 29) << "Subrun transition (hw=" << hw_subrun_trigger
								  << " sw=" << sw_subrun_trigger
								  << ") at EWT=" << fragment_timestamp
								  << " ev_counter=" << ev_counter()
								  << " -> subrun " << next_subrun;
			publishRecord_();
			subrun_record_.reset(next_subrun);
			metricMan->sendMetric("SubrunNumber", static_cast<uint64_t>(subrun_record_.subrun_number), "subrun", 1,
								  artdaq::MetricMode::LastPoint | artdaq::MetricMode::Persist);
			//                                                                   next EWT             subrun       ID
			frags.emplace_back(artdaq::MetadataFragment::CreateEndOfSubrunFragment(
				my_rank, fragment_timestamp + 1, next_subrun, fragment_id()));
		}
	}

	const auto after_copy = std::chrono::steady_clock::now();
	TLOG(TLVL_DEBUG + 27) << "Reporting Metrics";
	const auto hwTime = theCFO_->GetDevice()->GetDeviceTime();

	const double hw_timestamp_rate = 1. / hwTime;

	metricMan->sendMetric("CFO Read Time", artdaq::TimeUtils::GetElapsedTime(after_read, after_copy), "s", 3, artdaq::MetricMode::Average);
	metricMan->sendMetric("Fragment Prep Time", artdaq::TimeUtils::GetElapsedTime(before_read, after_read), "s", 3, artdaq::MetricMode::Average);
	metricMan->sendMetric("HW Timestamp Rate", hw_timestamp_rate, "timestamps/s", 1, artdaq::MetricMode::Average);

	TLOG(TLVL_DEBUG + 28) << "Returning true";

	return true;
}

size_t mu2e::CFODataReceiver::getCurrentSequenceID()
{
	return ev_counter();
}

// The following macro is defined in artdaq's GeneratorMacros.hh header
DEFINE_ARTDAQ_COMMANDABLE_GENERATOR(mu2e::CFODataReceiver)
