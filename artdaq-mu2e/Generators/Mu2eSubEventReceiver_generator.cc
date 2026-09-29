// This file reads out DTCs that are NOT in HW Event-building mode
// It can be used as an exmaple for developing more specific functionality.
//
// -----------------------------------------------------------------------------
// Detector-delay emulation
// -----------------------------------------------------------------------------
// A lognormal random delay can be injected before each fragment is returned,
// to emulate the natural timing spread of real detector readout chains.
//
// FHiCL parameters (all optional, feature is off by default):
//
//   throttle_lognormal_usecs  (double, default 0.0)
//       Mean delay in microseconds.  Set to 0 to disable the feature entirely.
//
//   throttle_lognormal_sigma  (double, default 0.5)
//       Shape of the distribution -- the standard deviation of the underlying
//       normal.  Larger values produce a heavier tail.  0.5 is a reasonable
//       starting point for a single detector subsystem.
//
//   throttle_lognormal_seed   (uint64, default 0)
//       Random seed for the generator.
//       0  ->  a unique seed is drawn produces an independent delay sequence.
//       >0 ->  a fixed deterministic seed; set the same value on every board
//              to force all detectors to produce the identical delay sequence
//              (same delay for event N on every board).
// -----------------------------------------------------------------------------
// Software-triggered subrun rollover
// -----------------------------------------------------------------------------
// Emits an EndOfSubrun metadata fragment every N events, from a single
// designated fragment ID. Off by default.
//
// FHiCL parameters (all optional, feature is off by default):
//
//   subrun_rollover_interval    (size_t, default 0)
//       Number of events between subrun transitions. 0 disables the feature.
//
//   subrun_rollover_fragment_id (int, default -1)
//       Only the generator instance whose fragment_id() equals this value
//       performs the rollover and emits the EndOfSubrun fragment. -1 disables
//       the feature regardless of subrun_rollover_interval.
// -----------------------------------------------------------------------------

#include "artdaq-core-mu2e/Overlays/FragmentType.hh"
#include "artdaq-core-mu2e/Overlays/DTCEventFragment.hh"
#include "dtcInterfaceLib/DTC.h"
#include "dtcInterfaceLib/DTCSoftwareCFO.h"

#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Data/MetadataFragment.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Generators/GeneratorMacros.hh"

#include <fstream>
#include <random>
#include <cmath>

#include "trace.h"
#define TRACE_NAME "Mu2eSubEventReceiver"

namespace mu2e {
class Mu2eSubEventReceiver : public artdaq::CommandableFragmentGenerator
{
public:
	explicit Mu2eSubEventReceiver(fhicl::ParameterSet const& ps);
	virtual ~Mu2eSubEventReceiver();

	DTCLib::DTC_SimMode GetMode() { return mode_; }

private:
	bool getNextDTCFragment(artdaq::FragmentPtrs& output, DTCLib::DTC_EventWindowTag ts);
	bool getNextDTCEventFragment(artdaq::FragmentPtrs& output, DTCLib::DTC_EventWindowTag ts);

	void start() override;

	void stopNoMutex() override {}

	void stop() override;

	void readSimFile_(std::string sim_file);

	size_t getCurrentSequenceID();

	/// Sleep for a random duration drawn from the configured distribution,
	/// honouring should_stop() so the delay is interruptible.
	void emulateDetectorDelay_();

	// Like "getNext_", "fragmentIDs_" is a mandatory override; it
	// returns a vector of the fragment IDs an instance of this class
	// is responsible for (in the case of Mu2eSubEventReceiver, this is just
	// the fragment_id_ variable declared in the parent
	// CommandableFragmentGenerator class)

	std::vector<artdaq::Fragment::fragment_id_t> fragmentIDs_() { return fragment_ids_; }

	std::vector<artdaq::Fragment::fragment_id_t> fragment_ids_;

	// State
	size_t current_timestamp_offset_{0};
	DTCLib::DTC_SimMode mode_;  //!=0 is simulation mode
	bool simFileRead_{true};
	const bool skip_dtc_init_;
	bool rawOutput_{false};
	std::string rawOutputFile_{""};
	std::ofstream rawOutputStream_;
	bool print_packets_;
	size_t heartbeats_after_{16};
	bool fail_on_dtc_read_error_{true};

	size_t dtc_offset_{0};
	size_t n_dtcs_{1};
	size_t first_timestamp_seen_{size_t(-1)}, last_fragment_timestamp_{size_t(-1)};

	std::unique_ptr<DTCLib::DTC> theInterface_;
	std::unique_ptr<DTCLib::DTCSoftwareCFO> theCFO_;

	std::size_t const throttle_usecs_;
	std::condition_variable throttle_cv_;
	std::mutex throttle_mutex_;

	// --- Emulated detector-delay parameters (lognormal) ---
	// throttle_lognormal_usecs: mean delay in us. 0 (default) = disabled.
	// throttle_lognormal_sigma: shape parameter of the underlying normal (default 0.5).
	// throttle_lognormal_seed:  0 = unique random seed per instance;
	//                           >0 = fixed seed (identical sequence on all instances sharing it).
	double const throttle_lognormal_usecs_;  // 0 means disabled
	double const throttle_lognormal_sigma_;
	std::mt19937_64 delay_rng_;
	std::lognormal_distribution<double> delay_lognorm_dist_;

	// --- Subrun rollover parameters ---
	// subrun_rollover_interval_: number of events between software-triggered
	//     subrun transitions. 0 disables the feature.
	// subrun_rollover_fragment_id_: only the generator instance whose
	//     fragment_id() matches this value performs the rollover and emits
	//     the EndOfSubrun fragment. -1 (default) disables the feature.
	std::size_t subrun_rollover_interval_;
	int subrun_rollover_fragment_id_;
	int subrun_number_{-1};
	// The "getNext_" function is used to implement user-specific
	// functionality; it's a mandatory override of the pure virtual
	// getNext_ function declared in CommandableFragmentGenerator

	bool getNext_(artdaq::FragmentPtrs& output) override;
	DTCLib::DTC_EventWindowTag getCurrentEventWindowTag();
};
}  // namespace mu2e

mu2e::Mu2eSubEventReceiver::~Mu2eSubEventReceiver() {}

bool mu2e::Mu2eSubEventReceiver::getNext_(artdaq::FragmentPtrs& frags)
{
	TLOG(TLVL_DEBUG + 30) << "getNext_";
	while (!simFileRead_ && !should_stop())
	{
		TLOG(TLVL_DEBUG + 31) << "Sleeping...";
		usleep(5000);
	}

	if (throttle_usecs_ > 0)
	{
		TLOG(TLVL_DEBUG + 32) << "Throttling... " << throttle_usecs_;
		std::unique_lock<std::mutex> throttle_lock(throttle_mutex_);
		throttle_cv_.wait_for(throttle_lock, std::chrono::microseconds(throttle_usecs_), [&]() { return should_stop(); });
	}

	if (throttle_lognormal_usecs_ > 0.0)
	{
		emulateDetectorDelay_();
	}

	if (should_stop())
	{
		TLOG(TLVL_DEBUG + 33) << "Stopping.";
		return false;
	}

	uint64_t z = 0;
	DTCLib::DTC_EventWindowTag zero(z);

	if (mode_ != DTCLib::DTC_SimMode_Disabled)
	{
		TLOG(TLVL_TRACE + 30) << "Sending request for timestamp " << getCurrentEventWindowTag().GetEventWindowTag(true);
		theCFO_->SendRequestForTimestamp(getCurrentEventWindowTag(), heartbeats_after_);
	}

	TLOG(TLVL_DEBUG + 34) << "getNext_ req";
	auto start_time = std::chrono::steady_clock::now();
	bool retVal = true;
	do
	{
		retVal = getNextDTCEventFragment(frags, zero);  // getNextDTCFragment(frags, zero);
		TLOG(TLVL_DEBUG + 35) << "getNext_ req retry? " << retVal << " " << frags.size();
	} while (1 && retVal && frags.size() < 900 &&
			 artdaq::TimeUtils::GetElapsedTimeMicroseconds(start_time) < 100000 /* 100 ms */);
	TLOG(TLVL_DEBUG + 36) << "getNext_ req done" << retVal << " " << frags.size();

	return retVal;
}  // end getNext_()

DTCLib::DTC_EventWindowTag mu2e::Mu2eSubEventReceiver::getCurrentEventWindowTag()
{
	if (first_timestamp_seen_ != size_t(-1))
	{
		return DTCLib::DTC_EventWindowTag(getCurrentSequenceID() + first_timestamp_seen_);
	}

	return DTCLib::DTC_EventWindowTag(uint64_t(0));
}

mu2e::Mu2eSubEventReceiver::Mu2eSubEventReceiver(fhicl::ParameterSet const& ps)
	: CommandableFragmentGenerator(ps)
	, fragment_ids_{static_cast<artdaq::Fragment::fragment_id_t>(fragment_id())}
	, mode_(DTCLib::DTC_SimModeConverter::ConvertToSimMode(ps.get<std::string>("sim_mode", "Disabled")))
	, skip_dtc_init_(ps.get<bool>("skip_dtc_init", false))
	, rawOutput_(ps.get<bool>("raw_output_enable", false))
	, rawOutputFile_(ps.get<std::string>("raw_output_file", "/tmp/Mu2eReceiver.bin"))
	, print_packets_(ps.get<bool>("debug_print", false))
	, heartbeats_after_(ps.get<size_t>("null_heartbeats_after_requests", 16))
	, fail_on_dtc_read_error_(ps.get<bool>("fail_on_dtc_read_error", true))
	, dtc_offset_(ps.get<size_t>("dtc_position_in_chain", 0))
	, n_dtcs_(ps.get<size_t>("n_dtcs_in_chain", 1))
	, throttle_usecs_(ps.get<size_t>("throttle_usecs", 0))  // in units of us
	, throttle_lognormal_usecs_(ps.get<double>("throttle_lognormal_usecs", 0.0))
	, throttle_lognormal_sigma_(ps.get<double>("throttle_lognormal_sigma", 0.5))
	, subrun_rollover_interval_(ps.get<std::size_t>("subrun_rollover_interval", 0))
	, subrun_rollover_fragment_id_(ps.get<int>("subrun_rollover_fragment_id", -1))
{
	// --- Initialise emulated-delay RNG ---
	if (throttle_lognormal_usecs_ > 0.0)
	{
		uint64_t seed = ps.get<uint64_t>("throttle_lognormal_seed", 0);
		if (seed == 0)
		{
			// Draw two 32-bit words from the hardware entropy source so that
			// each detector instance gets a unique, unpredictable sequence.
			std::random_device rd;
			seed = (static_cast<uint64_t>(rd()) << 32) | rd();
		}
		delay_rng_.seed(seed);
		// Back-compute the lognormal location parameter m so that the
		// distribution mean equals throttle_lognormal_usecs_.
		double m = std::log(throttle_lognormal_usecs_) - 0.5 * throttle_lognormal_sigma_ * throttle_lognormal_sigma_;
		delay_lognorm_dist_ = std::lognormal_distribution<double>(m, throttle_lognormal_sigma_);
		TLOG(TLVL_INFO) << "Detector delay emulation enabled: lognormal, mean="
						<< throttle_lognormal_usecs_ << " us, sigma=" << throttle_lognormal_sigma_
						<< ", seed=" << seed;
	}

	// mode_ can still be overridden by environment!
	theInterface_ = std::make_unique<DTCLib::DTC>(mode_,
												  ps.get<int>("dtc_id", -1),
												  ps.get<unsigned>("roc_mask", 0x1),
												  ps.get<std::string>("dtc_fw_version", ""),
												  skip_dtc_init_,
												  ps.get<std::string>("simulator_memory_file_name", "mu2esim.bin"));

	mode_ = theInterface_->GetSimMode();
	TLOG(TLVL_DEBUG) << "Mu2eSubEventReceiver Initialized with mode " << mode_;

	// if in simulation mode, setup CFO
	if (mode_ != 0)
	{
		fhicl::ParameterSet cfoConfig = ps.get<fhicl::ParameterSet>("cfo_config", fhicl::ParameterSet());
		theCFO_ = std::make_unique<DTCLib::DTCSoftwareCFO>(theInterface_.get(),
														   cfoConfig.get<bool>("use_dtc_cfo_emulator", true),
														   cfoConfig.get<size_t>("debug_packet_count", 0),
														   DTCLib::DTC_DebugTypeConverter::ConvertToDebugType(cfoConfig.get<std::string>("debug_type", "2")),
														   cfoConfig.get<bool>("sticky_debug_type", false),
														   cfoConfig.get<bool>("quiet", false),
														   cfoConfig.get<bool>("asyncRR", false),
														   cfoConfig.get<bool>("force_no_debug_mode", false),
														   cfoConfig.get<bool>("useCFODRP", false));
	}

	if (skip_dtc_init_)
	{
		try
		{
			theInterface_->ReleaseAllBuffers(DTC_DMA_Engine_DAQ);
		}
		catch (const std::exception& ex)
		{
			throw std::runtime_error(std::string("Mu2eSubEventReceiver: ReleaseAllBuffers failed during init (skip_dtc_init): ") + ex.what());
		}
		catch (...)
		{
			throw std::runtime_error("Mu2eSubEventReceiver: ReleaseAllBuffers failed during init (skip_dtc_init): unknown exception");
		}
		return;  // skip any control of DTC
	}

	if (ps.get<bool>("load_sim_file", false))
	{
		theInterface_->SetDetectorEmulatorInUse();
		theInterface_->ResetDDR();
		theInterface_->SoftReset();

		char* file_c = getenv("DTCLIB_SIM_FILE");

		auto sim_file = ps.get<std::string>("sim_file", "");
		if (file_c != nullptr)
		{
			sim_file = std::string(file_c);
		}
		if (sim_file.size() > 0)
		{
			simFileRead_ = false;
			std::thread reader(&mu2e::Mu2eSubEventReceiver::readSimFile_, this, sim_file);
			reader.detach();
		}
	}
	else
	{
		theInterface_->ClearDetectorEmulatorInUse();  // Needed if we're doing ROC Emulator...make sure Detector Emulation
		// is disabled
		try
		{
			theInterface_->ReleaseAllBuffers(DTC_DMA_Engine_DAQ);
		}
		catch (const std::exception& ex)
		{
			throw std::runtime_error(std::string("Mu2eSubEventReceiver: ReleaseAllBuffers failed during init: ") + ex.what());
		}
		catch (...)
		{
			throw std::runtime_error("Mu2eSubEventReceiver: ReleaseAllBuffers failed during init: unknown exception");
		}
	}
}

void mu2e::Mu2eSubEventReceiver::readSimFile_(std::string sim_file)
{
	TLOG(TLVL_INFO) << "Starting read of simulation file " << sim_file << "."
					<< " Please wait to start the run until finished.";
	theInterface_->WriteSimFileToDTC(sim_file, true);
	simFileRead_ = true;
	TLOG(TLVL_INFO) << "Done reading simulation file into DTC memory.";
}

void mu2e::Mu2eSubEventReceiver::stop()
{
	rawOutputStream_.close();

	if (skip_dtc_init_) return;  // skip any control of DTC

	theInterface_->DisableDetectorEmulator();
	theInterface_->DisableCFOEmulation();
}

void mu2e::Mu2eSubEventReceiver::start()
{
	theInterface_->ReleaseAllBuffers(DTC_DMA_Engine_DAQ);

	subrun_number_ = subrun_number();

	if (rawOutput_)
	{
		std::string fileName = rawOutputFile_;
		if (fileName.find(".bin") != std::string::npos)
		{
			std::string timestr = "_" + std::to_string(time(0));
			fileName.insert(fileName.find(".bin"), timestr);
		}
		rawOutputStream_.open(fileName, std::ios::out | std::ios::app | std::ios::binary);
	}
}

void mu2e::Mu2eSubEventReceiver::emulateDetectorDelay_()
{
	double delay_us = delay_lognorm_dist_(delay_rng_);

	auto delay_int = static_cast<uint64_t>(delay_us);
	TLOG(TLVL_DEBUG + 40) << "emulateDetectorDelay_: delaying " << delay_int << " us";

	// Reuse the existing throttle mutex/cv so the sleep is interruptible by stop().
	std::unique_lock<std::mutex> lock(throttle_mutex_);
	throttle_cv_.wait_for(lock, std::chrono::microseconds(delay_int), [&]() { return should_stop(); });
}

bool mu2e::Mu2eSubEventReceiver::getNextDTCFragment(artdaq::FragmentPtrs& frags, DTCLib::DTC_EventWindowTag ts_in)
{
	auto before_read = std::chrono::steady_clock::now();
	int retryCount = 5;
	std::vector<std::unique_ptr<DTCLib::DTC_SubEvent>> data;
	while (data.size() == 0 && retryCount >= 0)
	{
		try
		{
			data = theInterface_->GetSubEventData(ts_in /* not used when not matching */);
			TLOG(TLVL_DEBUG + 24) << "Done calling theInterface->GetData() data.size()=" << data.size() << ", retryCount=" << retryCount;
		}
		catch (std::exception const& ex)
		{
			TLOG(TLVL_ERROR) << "There was an error in the DTC Library: " << ex.what();
			if (fail_on_dtc_read_error_)
			{
				throw std::runtime_error(std::string("Mu2eSubEventReceiver fatal DTC read error: ") + ex.what());
			}
		}
		retryCount--;
	}
	if (retryCount < 0 && data.size() == 0)
	{
		// Return true if no data in external CFO mode, otherwise false
		return mode_ == 0;
	}
	auto after_read = std::chrono::steady_clock::now();

	DTCLib::DTC_EventWindowTag ts_out = data[0]->GetEventWindowTag();
	TLOG(TLVL_TRACE + 19) << "Received data with timestamp " << ts_out.GetEventWindowTag(true);

	// GetSubEventData can return multiple EWTs, and we can assume that there is ONE DTC_SubEvent per EWT!
	for (auto& subevt : data)
	{
		ts_out = subevt->GetEventWindowTag();
		TLOG(TLVL_TRACE + 19) << "Processing subevent with timestamp " << ts_out.GetEventWindowTag(true);
		size_t size_bytes = sizeof(DTCLib::DTC_EventHeader);
		size_bytes += subevt->GetSubEventByteCount();
		TLOG(TLVL_DEBUG + 20) << "Size of event will be " << static_cast<int>(size_bytes) << "B";

		auto evt = std::make_unique<DTCLib::DTC_Event>(size_bytes);

		DTCLib::DTC_EventHeader evtHdr;
		evtHdr.inclusive_event_byte_count = size_bytes;
		evtHdr.num_dtcs = 1;
		evtHdr.event_tag_low = ts_out.GetEventWindowTag(true) & 0xFFFFFFFF;
		evtHdr.event_tag_high = (ts_out.GetEventWindowTag(true) >> 32) & 0xFFFF;
		memcpy(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(evt->GetRawBufferPointer())), &evtHdr, sizeof(DTCLib::DTC_EventHeader));
		auto ptr = reinterpret_cast<const uint8_t*>(evt->GetRawBufferPointer()) + sizeof(DTCLib::DTC_EventHeader);

		TLOG(TLVL_DEBUG + 21) << "Calling memcpy(" << (const void*)ptr << ", " << (void*)subevt->GetRawBufferPointer() << ", " << subevt->GetSubEventByteCount() << ")";
		memcpy(const_cast<uint8_t*>(ptr), subevt->GetRawBufferPointer(), subevt->GetSubEventByteCount());
		ptr += subevt->GetSubEventByteCount();
		TLOG(TLVL_DEBUG + 22) << "Calling SetupEvent";
		evt->SetupEvent();
		TLOG(TLVL_DEBUG + 23) << "Setting EventWindowTag to " << ts_out.GetEventWindowTag(true);
		evt->SetEventWindowTag(ts_out);

		for (size_t se = 0; se < evt->GetSubEventCount(); ++se)
		{
			auto subevt = evt->GetSubEvent(se);
			auto subevtheader = subevt->GetHeader();
			metricMan->sendMetric("ROC link0 status", subevtheader->link0_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link1 status", subevtheader->link1_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link2 status", subevtheader->link2_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link3 status", subevtheader->link3_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link4 status", subevtheader->link4_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link5 status", subevtheader->link5_status, "status", 3, artdaq::MetricMode::Maximum);

			metricMan->sendMetric("ROC link0 latency", subevtheader->link0_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link1 latency", subevtheader->link1_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link2 latency", subevtheader->link2_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link3 latency", subevtheader->link3_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link4 latency", subevtheader->link4_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link5 latency", subevtheader->link5_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);

			for (size_t bl = 0; bl < subevt->GetDataBlockCount(); ++bl)
			{
				auto block = subevt->GetDataBlock(bl);
				auto first = block->GetHeader();
				std::string nn = "Packets per ROC " + std::to_string(bl);
				metricMan->sendMetric(nn, first->GetPacketCount(), "Packages", 3, artdaq::MetricMode::Average);
				std::string rocLink = "ROC " + std::to_string(first->GetLinkID()) + " status";
				metricMan->sendMetric(rocLink, first->GetStatus(), "status", 3, artdaq::MetricMode::Maximum);
			}
		}

		if (print_packets_)
		{
			TLOG(TLVL_INFO) << "[print_packets starts] subEventCounts: " << evt->GetSubEventCount();
			for (size_t se = 0; se < evt->GetSubEventCount(); ++se)
			{
				auto subevt = evt->GetSubEvent(se);
				auto subevtheader = subevt->GetHeader();
				TLOG(TLVL_INFO) << subevtheader->toJson();
				for (size_t bl = 0; bl < subevt->GetDataBlockCount(); ++bl)
				{
					auto block = subevt->GetDataBlock(bl);
					auto first = block->GetHeader();
					TLOG(TLVL_INFO) << first->toJSON();
					for (int ii = 0; ii < first->GetPacketCount(); ++ii)
					{
						TLOG(TLVL_INFO) << DTCLib::DTC_DataPacket(((uint8_t*)block->blockPointer) + ((ii + 1) * 16)).toJSON();
					}
				}
			}
		}
		if (rawOutput_)
		{
			evt->WriteEvent(rawOutputStream_, false);
		}

		// auto after_print = std::chrono::steady_clock::now();

		auto fragment_timestamp = ts_out.GetEventWindowTag(true);
		//      auto timestamp_to_use = fragment_timestamp + current_timestamp_offset_;
		metricMan->sendMetric("Last event window tag", fragment_timestamp, "status", 3, artdaq::MetricMode::LastPoint);

		// if (last_fragment_timestamp_ != size_t(-1) && last_fragment_timestamp_ + 1 != timestamp_to_use)
		//      {
		//        TLOG(TLVL_DEBUG+19) << "NOT INCREMENTAL timestamp new=" << timestamp_to_use << " vs old=" << last_fragment_timestamp_;
		//      }

		// if (first_timestamp_seen_ == size_t(-1)) //reset
		//      {
		//        first_timestamp_seen_   = fragment_timestamp;
		//      }

		// if (timestamp_to_use < last_fragment_timestamp_)
		//      {
		//        current_timestamp_offset_ = last_fragment_timestamp_ - fragment_timestamp + 1;  // So that this == last_fragment_timestamp_ + 1
		//        timestamp_to_use = fragment_timestamp + current_timestamp_offset_;
		//      }
		// last_fragment_timestamp_ = timestamp_to_use;
		// TLOG(TLVL_DEBUG + 24) << "fragment_timestamp=" << fragment_timestamp << " while timestamp_to_use=" << timestamp_to_use;

		// frags.emplace_back(new artdaq::Fragment(getCurrentSequenceID(), fragment_ids_[0], FragmentType::DTCEVT, fragment_timestamp));
		TLOG(TLVL_DEBUG + 25) << "Creating Fragment, sz=" << evt->GetEventByteCount() << ", frag_id=" << fragment_ids_[0]
							  << ", timestamp=" << fragment_timestamp;

		frags.emplace_back(new artdaq::Fragment(fragment_timestamp, fragment_ids_[0], FragmentType::DTCEVT, fragment_timestamp));
		frags.back()->resizeBytes(evt->GetEventByteCount());

		if (evt->IsCorrupt())
		{
			DTCEventFragment::Metadata md;
			md.corrupt_flag = true;
			frags.back()->setMetadata(md);
		}
		memcpy(frags.back()->dataBegin(), evt->GetRawBufferPointer(), evt->GetEventByteCount());
		metricMan->sendMetric("Average Event Size", evt->GetEventByteCount(), "Bytes", 3, artdaq::MetricMode::Average);
		TLOG(TLVL_DEBUG + 26) << "Incrementing event counter";
		ev_counter_inc();
	}
	auto after_copy = std::chrono::steady_clock::now();
	for (auto& frag : frags) { frag->getLatency(true); }
	TLOG(TLVL_DEBUG + 27) << "Reporting Metrics";
	auto hwTime = theInterface_->GetDevice()->GetDeviceTime();

	double hw_timestamp_rate = 1 / hwTime;
	double hw_data_rate = frags.back()->sizeBytes() / hwTime;

	metricMan->sendMetric("DTC Read Time", artdaq::TimeUtils::GetElapsedTime(after_read, after_copy), "s", 3, artdaq::MetricMode::Average);
	metricMan->sendMetric("Fragment Prep Time", artdaq::TimeUtils::GetElapsedTime(before_read, after_read), "s", 3, artdaq::MetricMode::Average);
	metricMan->sendMetric("HW Timestamp Rate", hw_timestamp_rate, "timestamps/s", 1, artdaq::MetricMode::Average);
	metricMan->sendMetric("PCIe Transfer Rate", hw_data_rate, "B/s", 1, artdaq::MetricMode::Average);

	TLOG(TLVL_DEBUG + 28) << "Returning true";

	return true;
}

bool mu2e::Mu2eSubEventReceiver::getNextDTCEventFragment(artdaq::FragmentPtrs& frags, DTCLib::DTC_EventWindowTag ts_in)
{
	auto before_read = std::chrono::steady_clock::now();
	int retryCount = 5;
	std::vector<std::shared_ptr<DTCLib::DTC_Event>> events;
	while (events.size() == 0 && retryCount >= 0)
	{
		try
		{
			events = theInterface_->GetSubEventDataAsEvents(ts_in);
			TLOG(TLVL_DEBUG + 24) << "Done calling theInterface->GetSubEventDataAsEvents() events.size()=" << events.size() << ", retryCount=" << retryCount;
		}
		catch (std::exception const& ex)
		{
			TLOG(TLVL_ERROR) << "There was an error in the DTC Library: " << ex.what();
			if (fail_on_dtc_read_error_)
			{
				throw std::runtime_error(std::string("Mu2eSubEventReceiver fatal DTC read error: ") + ex.what());
			}
		}
		retryCount--;
	}
	if (retryCount < 0 && events.size() == 0)
	{
		return mode_ == 0;
	}
	auto after_read = std::chrono::steady_clock::now();

	for (auto& evt : events)
	{
		DTCLib::DTC_EventWindowTag ts_out = evt->GetEventWindowTag();
		TLOG(TLVL_TRACE + 19) << "Processing event with timestamp " << ts_out.GetEventWindowTag(true);
		TLOG(TLVL_DEBUG + 20) << "Size of event is " << static_cast<int>(evt->GetEventByteCount()) << "B";

		for (size_t se = 0; se < evt->GetSubEventCount(); ++se)
		{
			auto subevt = evt->GetSubEvent(se);
			auto subevtheader = subevt->GetHeader();
			metricMan->sendMetric("ROC link0 status", subevtheader->link0_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link1 status", subevtheader->link1_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link2 status", subevtheader->link2_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link3 status", subevtheader->link3_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link4 status", subevtheader->link4_status, "status", 3, artdaq::MetricMode::Maximum);
			metricMan->sendMetric("ROC link5 status", subevtheader->link5_status, "status", 3, artdaq::MetricMode::Maximum);

			metricMan->sendMetric("ROC link0 latency", subevtheader->link0_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link1 latency", subevtheader->link1_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link2 latency", subevtheader->link2_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link3 latency", subevtheader->link3_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link4 latency", subevtheader->link4_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);
			metricMan->sendMetric("ROC link5 latency", subevtheader->link5_drp_rx_latency, "status", 3, artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum | artdaq::MetricMode::Average);

			for (size_t bl = 0; bl < subevt->GetDataBlockCount(); ++bl)
			{
				auto block = subevt->GetDataBlock(bl);
				auto first = block->GetHeader();
				std::string nn = "Packets per ROC " + std::to_string(bl);
				metricMan->sendMetric(nn, first->GetPacketCount(), "Packages", 3, artdaq::MetricMode::Average);
				std::string rocLink = "ROC " + std::to_string(first->GetLinkID()) + " status";
				metricMan->sendMetric(rocLink, first->GetStatus(), "status", 3, artdaq::MetricMode::Maximum);
			}
		}

		if (print_packets_)
		{
			TLOG(TLVL_INFO) << "[print_packets starts] subEventCounts: " << evt->GetSubEventCount();
			for (size_t se = 0; se < evt->GetSubEventCount(); ++se)
			{
				auto subevt = evt->GetSubEvent(se);
				auto subevtheader = subevt->GetHeader();
				TLOG(TLVL_INFO) << subevtheader->toJson();
				for (size_t bl = 0; bl < subevt->GetDataBlockCount(); ++bl)
				{
					auto block = subevt->GetDataBlock(bl);
					auto first = block->GetHeader();
					TLOG(TLVL_INFO) << first->toJSON();
					for (int ii = 0; ii < first->GetPacketCount(); ++ii)
					{
						TLOG(TLVL_INFO) << DTCLib::DTC_DataPacket(((uint8_t*)block->blockPointer) + ((ii + 1) * 16)).toJSON();
					}
				}
			}
		}
		if (rawOutput_)
		{
			evt->WriteEvent(rawOutputStream_, false);
		}

		auto fragment_timestamp = ts_out.GetEventWindowTag(true);
		metricMan->sendMetric("Last event window tag", fragment_timestamp, "status", 3, artdaq::MetricMode::LastPoint);

		TLOG(TLVL_DEBUG + 25) << "Creating Fragment, sz=" << evt->GetEventByteCount() << ", frag_id=" << fragment_ids_[0]
							  << ", timestamp=" << fragment_timestamp;

		frags.emplace_back(new artdaq::Fragment(fragment_timestamp, fragment_ids_[0], FragmentType::DTCEVT, fragment_timestamp));
		frags.back()->resizeBytes(evt->GetEventByteCount());

		if (evt->IsCorrupt())
		{
			DTCEventFragment::Metadata md;
			md.corrupt_flag = true;
			frags.back()->setMetadata(md);
		}
		memcpy(frags.back()->dataBegin(), evt->GetRawBufferPointer(), evt->GetEventByteCount());
		metricMan->sendMetric("Average Event Size", evt->GetEventByteCount(), "Bytes", 3, artdaq::MetricMode::Average);
		TLOG(TLVL_DEBUG + 26) << "Incrementing event counter";
		ev_counter_inc();

		//--------------------------------------------------------------------------------
		// Sub-run transition: fire inline so the boundary is exact within the batch.
		// Only the instance whose fragment_id() matches subrun_rollover_fragment_id_
		// performs the rollover; -1 (default) disables the feature entirely.
		//--------------------------------------------------------------------------------
		const bool sw_subrun_trigger = (subrun_rollover_fragment_id_ >= 0) &&
									   (static_cast<int>(fragment_id()) == subrun_rollover_fragment_id_) &&
									   (subrun_rollover_interval_ > 0) &&
									   (ev_counter() % subrun_rollover_interval_ == 0);
		if (sw_subrun_trigger)
		{
			const auto next_subrun = subrun_number_ + 1;
			TLOG(TLVL_DEBUG + 29) << "Subrun transition at EWT=" << fragment_timestamp
								  << " ev_counter=" << ev_counter()
								  << " -> subrun " << next_subrun;
			subrun_number_ = next_subrun;
			//                                                                   next EWT             subrun       ID
			frags.emplace_back(artdaq::MetadataFragment::CreateEndOfSubrunFragment(
				my_rank, fragment_timestamp + 1, next_subrun, fragment_id()));
		}
	}
	auto after_copy = std::chrono::steady_clock::now();
	for (auto& frag : frags) { frag->getLatency(true); }
	TLOG(TLVL_DEBUG + 27) << "Reporting Metrics";
	auto hwTime = theInterface_->GetDevice()->GetDeviceTime();

	double hw_timestamp_rate = 1 / hwTime;
	double hw_data_rate = frags.back()->sizeBytes() / hwTime;

	metricMan->sendMetric("DTC Read Time", artdaq::TimeUtils::GetElapsedTime(after_read, after_copy), "s", 3, artdaq::MetricMode::Average);
	metricMan->sendMetric("Fragment Prep Time", artdaq::TimeUtils::GetElapsedTime(before_read, after_read), "s", 3, artdaq::MetricMode::Average);
	metricMan->sendMetric("HW Timestamp Rate", hw_timestamp_rate, "timestamps/s", 1, artdaq::MetricMode::Average);
	metricMan->sendMetric("PCIe Transfer Rate", hw_data_rate, "B/s", 1, artdaq::MetricMode::Average);

	TLOG(TLVL_DEBUG + 28) << "Returning true";

	return true;
}

size_t mu2e::Mu2eSubEventReceiver::getCurrentSequenceID()
{
	return ev_counter();
}

// The following macro is defined in artdaq's GeneratorMacros.hh header
DEFINE_ARTDAQ_COMMANDABLE_GENERATOR(mu2e::Mu2eSubEventReceiver)
