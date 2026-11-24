#include "TRACE/tracemf.h"

#include "artdaq/ArtModules/ArtdaqRunInfoServiceInterface.h"

#include "art/Framework/Services/Registry/ServiceDefinitionMacros.h"

#include "fhiclcpp/ParameterSet.h"

#include <libpq-fe.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#define TRACE_NAME "Mu2eRunInfoDbService"

// Required Postgres tables (create manually before first use):
//
// CREATE TABLE IF NOT EXISTS <schema>.subrun_datastream (
//     id              BIGSERIAL PRIMARY KEY,
//     run_number      INTEGER NOT NULL,
//     subrun_number   INTEGER NOT NULL,
//     n_events        BIGINT NOT NULL DEFAULT 0,
//     first_event     BIGINT NOT NULL DEFAULT 0,
//     last_event      BIGINT NOT NULL DEFAULT 0,
//     datastream      TEXT NOT NULL DEFAULT 'default',
//     created_at      TIMESTAMP WITH TIME ZONE DEFAULT now()
// );
// CREATE INDEX idx_subrun_datastream_lookup
//     ON <schema>.subrun_datastream (run_number, subrun_number, datastream);
//
// CREATE TABLE IF NOT EXISTS <schema>.file_summary (
//     id              BIGSERIAL PRIMARY KEY,
//     file_name       TEXT NOT NULL,
//     run_number      INTEGER NOT NULL,
//     first_subrun    INTEGER NOT NULL,
//     last_subrun     INTEGER NOT NULL,
//     n_events        BIGINT NOT NULL DEFAULT 0,
//     file_size       BIGINT NOT NULL DEFAULT 0,
//     metadata        JSONB NOT NULL DEFAULT '{}',
//     datastream      TEXT NOT NULL DEFAULT 'default',
//     created_at      TIMESTAMP WITH TIME ZONE DEFAULT now()
// );
// CREATE INDEX idx_file_summary_lookup
//     ON <schema>.file_summary (file_name, datastream);
//
// CREATE TABLE IF NOT EXISTS <schema>.file_subrun (
//     id                BIGSERIAL PRIMARY KEY,
//     file_summary_id   BIGINT NOT NULL,
//     subrun_id         BIGINT NOT NULL,
//     created_at        TIMESTAMP WITH TIME ZONE DEFAULT now()
// );
// CREATE INDEX idx_file_subrun_file ON <schema>.file_subrun (file_summary_id);
// CREATE INDEX idx_file_subrun_subrun ON <schema>.file_subrun (subrun_id);

struct SubrunEntry
{
	art::RunNumber_t run;
	art::SubRunNumber_t subrun;
	size_t nEvents;
	art::EventNumber_t firstEvent;
	art::EventNumber_t lastEvent;
	std::string datastream;
};

struct FileSummaryEntry
{
	std::string fileName;
	art::RunNumber_t run;
	art::SubRunNumber_t firstSubrun;
	art::SubRunNumber_t lastSubrun;
	size_t nEvents;
	size_t fileSize;
	std::string metadata;
	std::string datastream;
};

using DbEntry = std::variant<SubrunEntry, FileSummaryEntry>;

class Mu2eRunInfoDbService : public ArtdaqRunInfoServiceInterface
{
public:
	Mu2eRunInfoDbService(fhicl::ParameterSet const& pset, art::ActivityRegistry&);
	~Mu2eRunInfoDbService() override;

	bool addSubrunRecord(
	    art::RunNumber_t run,
	    art::SubRunNumber_t subrun,
	    size_t nEvents,
	    art::EventNumber_t firstEvent,
	    art::EventNumber_t lastEvent,
	    std::string const& datastream) override;

	bool addFileSummary(
	    std::string const& fileName,
	    art::RunNumber_t run,
	    art::SubRunNumber_t firstSubrun,
	    art::SubRunNumber_t lastSubrun,
	    size_t nEvents,
	    size_t fileSize,
	    std::string const& metadata,
	    std::string const& datastream) override;

private:
	void dbWriterThread_();
	void writeSubrunToDb_(PGconn* conn, SubrunEntry const& entry);
	void writeFileSummaryToDb_(PGconn* conn, FileSummaryEntry const& entry);

	// Subrun IDs accumulated between subrun inserts and the next file summary insert
	std::vector<long long> pendingSubrunIds_;

	std::string summaryDir_;

	std::string connstr_;
	std::string dbSchema_;

	static constexpr size_t kDbQueueMaxSize = 20;
	std::queue<DbEntry> dbQueue_;
	std::mutex dbMutex_;
	std::condition_variable dbCv_;
	std::atomic<bool> dbStop_{false};
	std::thread dbThread_;
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(Mu2eRunInfoDbService, ArtdaqRunInfoServiceInterface, LEGACY)

Mu2eRunInfoDbService::Mu2eRunInfoDbService(fhicl::ParameterSet const& pset, art::ActivityRegistry& /*unused*/)
    : summaryDir_(pset.get<std::string>("summaryDir", ""))
    , connstr_(pset.get<std::string>("connstr", ""))
    , dbSchema_(pset.get<std::string>("dbSchema", ""))
{
	if (connstr_.empty())
	{
		char const* host = std::getenv("OTSDAQ_RUNINFO_DATABASE_HOST");
		char const* db   = std::getenv("OTSDAQ_RUNINFO_DATABASE");
		char const* port = std::getenv("OTSDAQ_RUNINFO_DATABASE_PORT");
		char const* user = std::getenv("OTSDAQ_RUNINFO_DATABASE_USER");
		if (host && db && port && user)
		{
			std::ostringstream oss;
			oss << "host=" << host << " port=" << port
			    << " dbname=" << db << " user=" << user;
			connstr_ = oss.str();
		}
	}

	if (dbSchema_.empty())
	{
		char const* schema_env = std::getenv("OTSDAQ_RUNINFO_DATABASE_SCHEMA");
		dbSchema_ = schema_env ? schema_env : "online";
	}

	TLOG(TLVL_INFO) << "Mu2eRunInfoDbService: summaryDir=\"" << summaryDir_
	                 << "\" connstr=" << (connstr_.empty() ? "(none)" : "(set)")
	                 << " schema=\"" << dbSchema_ << "\"";

	if (!connstr_.empty())
	{
		dbStop_.store(false);
		dbThread_ = std::thread([this]() { dbWriterThread_(); });
	}
}

Mu2eRunInfoDbService::~Mu2eRunInfoDbService()
{
	if (dbThread_.joinable())
	{
		dbStop_.store(true);
		dbCv_.notify_one();
		dbThread_.join();
	}
}

bool Mu2eRunInfoDbService::addSubrunRecord(
    art::RunNumber_t run,
    art::SubRunNumber_t subrun,
    size_t nEvents,
    art::EventNumber_t firstEvent,
    art::EventNumber_t lastEvent,
    std::string const& datastream)
{
	if (!summaryDir_.empty())
	{
		std::ostringstream fname;
		fname << summaryDir_;
		if (summaryDir_.back() != '/') { fname << '/'; }
		fname << "subrun_record_run" << std::setw(6) << std::setfill('0') << run << ".csv";

		std::ostringstream row;
		row << run << "," << subrun << "," << nEvents << "," << firstEvent << "," << lastEvent
		    << "," << datastream << "\n";

		appendToCsv(fname.str(),
		    "run,subrun,n_events,first_event,last_event,datastream\n", row.str());
	}

	if (!connstr_.empty())
	{
		std::lock_guard lock(dbMutex_);
		if (dbQueue_.size() >= kDbQueueMaxSize)
		{
			TLOG(TLVL_WARNING) << "DB queue full, dropping subrun record " << subrun;
		}
		else
		{
			dbQueue_.push(SubrunEntry{run, subrun, nEvents, firstEvent, lastEvent, datastream});
			dbCv_.notify_one();
		}
	}

	return true;
}

bool Mu2eRunInfoDbService::addFileSummary(
    std::string const& fileName,
    art::RunNumber_t run,
    art::SubRunNumber_t firstSubrun,
    art::SubRunNumber_t lastSubrun,
    size_t nEvents,
    size_t fileSize,
    std::string const& metadata,
    std::string const& datastream)
{
	if (!summaryDir_.empty())
	{
		std::ostringstream fname;
		fname << summaryDir_;
		if (summaryDir_.back() != '/') { fname << '/'; }
		fname << "file_summary_run" << std::setw(6) << std::setfill('0') << run << ".csv";

		std::string const outputFile = std::filesystem::path(fileName).filename().string();

		std::ostringstream row;
		row << outputFile << "," << run << "," << firstSubrun << "," << lastSubrun
		    << "," << nEvents << "," << fileSize << "," << datastream << "\n";

		appendToCsv(fname.str(),
		    "file_name,run,first_subrun,last_subrun,n_events,file_size,datastream\n", row.str());
	}

	if (!connstr_.empty())
	{
		std::lock_guard lock(dbMutex_);
		if (dbQueue_.size() >= kDbQueueMaxSize)
		{
			TLOG(TLVL_WARNING) << "DB queue full, dropping file summary for " << fileName;
		}
		else
		{
			dbQueue_.push(FileSummaryEntry{fileName, run, firstSubrun, lastSubrun,
			    nEvents, fileSize, metadata, datastream});
			dbCv_.notify_one();
		}
	}

	return true;
}

void Mu2eRunInfoDbService::dbWriterThread_()
{
	PGconn* conn = PQconnectdb(connstr_.c_str());
	if (PQstatus(conn) != CONNECTION_OK)
	{
		TLOG(TLVL_WARNING) << "DB connection failed: " << PQerrorMessage(conn)
		                   << " — records will not be written to DB";
		PQfinish(conn);

		while (true)
		{
			std::unique_lock lock(dbMutex_);
			dbCv_.wait(lock, [&] { return !dbQueue_.empty() || dbStop_.load(); });
			while (!dbQueue_.empty()) { dbQueue_.pop(); }
			if (dbStop_.load()) { break; }
		}
		return;
	}

	TLOG(TLVL_DEBUG) << "DB writer thread connected";

	while (true)
	{
		DbEntry entry;
		{
			std::unique_lock lock(dbMutex_);
			dbCv_.wait(lock, [&] { return !dbQueue_.empty() || dbStop_.load(); });
			if (dbQueue_.empty()) { break; }
			entry = std::move(dbQueue_.front());
			dbQueue_.pop();
		}

		if (PQstatus(conn) != CONNECTION_OK)
		{
			PQreset(conn);
			if (PQstatus(conn) != CONNECTION_OK)
			{
				TLOG(TLVL_WARNING) << "DB reconnect failed, dropping record";
				continue;
			}
		}

		std::visit([&](auto const& rec) {
			using T = std::decay_t<decltype(rec)>;
			if constexpr (std::is_same_v<T, SubrunEntry>)
			{
				writeSubrunToDb_(conn, rec);
			}
			else
			{
				writeFileSummaryToDb_(conn, rec);
			}
		}, entry);
	}

	PQfinish(conn);
	TLOG(TLVL_DEBUG) << "DB writer thread exiting";
}

void Mu2eRunInfoDbService::writeSubrunToDb_(PGconn* conn, SubrunEntry const& e)
{
	std::string const sql =
	    "INSERT INTO " + dbSchema_ + ".subrun_datastream "
	    "(run_number, subrun_number, n_events, first_event, last_event, datastream) "
	    "VALUES ($1,$2,$3,$4,$5,$6) RETURNING id";

	std::string const p1 = std::to_string(e.run);
	std::string const p2 = std::to_string(e.subrun);
	std::string const p3 = std::to_string(e.nEvents);
	std::string const p4 = std::to_string(e.firstEvent);
	std::string const p5 = std::to_string(e.lastEvent);
	std::string const& p6 = e.datastream;

	char const* params[6] = {p1.c_str(), p2.c_str(), p3.c_str(),
	                         p4.c_str(), p5.c_str(), p6.c_str()};

	PGresult* res = PQexecParams(conn, sql.c_str(), 6, nullptr, params, nullptr, nullptr, 0);
	if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
	{
		pendingSubrunIds_.push_back(std::atoll(PQgetvalue(res, 0, 0)));
	}
	else
	{
		TLOG(TLVL_WARNING) << "DB insert failed for subrun " << e.subrun
		                   << ": " << PQresultErrorMessage(res);
	}
	PQclear(res);
}

void Mu2eRunInfoDbService::writeFileSummaryToDb_(PGconn* conn, FileSummaryEntry const& e)
{
	std::string const outputFile = std::filesystem::path(e.fileName).filename().string();

	std::string const sql =
	    "INSERT INTO " + dbSchema_ + ".file_summary "
	    "(file_name, run_number, first_subrun, last_subrun, n_events, file_size, "
	    " metadata, datastream) "
	    "VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8) RETURNING id";

	std::string const p1 = outputFile;
	std::string const p2 = std::to_string(e.run);
	std::string const p3 = std::to_string(e.firstSubrun);
	std::string const p4 = std::to_string(e.lastSubrun);
	std::string const p5 = std::to_string(e.nEvents);
	std::string const p6 = std::to_string(e.fileSize);
	std::string const& p7 = e.metadata;
	std::string const& p8 = e.datastream;

	char const* params[8] = {p1.c_str(), p2.c_str(), p3.c_str(), p4.c_str(),
	                         p5.c_str(), p6.c_str(), p7.c_str(), p8.c_str()};

	PGresult* res = PQexecParams(conn, sql.c_str(), 8, nullptr, params, nullptr, nullptr, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1)
	{
		TLOG(TLVL_WARNING) << "DB insert failed for file " << outputFile
		                   << ": " << PQresultErrorMessage(res);
		PQclear(res);
		pendingSubrunIds_.clear();
		return;
	}

	long long fileId = std::atoll(PQgetvalue(res, 0, 0));
	PQclear(res);

	std::string const junctionSql =
	    "INSERT INTO " + dbSchema_ + ".file_subrun "
	    "(file_summary_id, subrun_id) VALUES ($1,$2)";

	std::string const fileIdStr = std::to_string(fileId);
	for (long long subrunId : pendingSubrunIds_)
	{
		std::string const subrunIdStr = std::to_string(subrunId);
		char const* jparams[2] = {fileIdStr.c_str(), subrunIdStr.c_str()};

		PGresult* jres = PQexecParams(conn, junctionSql.c_str(), 2, nullptr, jparams, nullptr, nullptr, 0);
		if (PQresultStatus(jres) != PGRES_COMMAND_OK)
		{
			TLOG(TLVL_WARNING) << "DB insert failed for file_subrun link file=" << fileId
			                   << " subrun=" << subrunId << ": " << PQresultErrorMessage(jres);
		}
		PQclear(jres);
	}

	pendingSubrunIds_.clear();
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(Mu2eRunInfoDbService, ArtdaqRunInfoServiceInterface)
