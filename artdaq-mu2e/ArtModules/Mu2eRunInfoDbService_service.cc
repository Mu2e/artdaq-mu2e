// Mu2eRunInfoDbService
//
// Loads the per-subrun records that RootDAQOutMF writes when an output file is
// closed into the run-info database.
//
// RootDAQOutMF (artdaq) appends one row per subrun to
//   <subrunRecordDir>/subrun_record_run<NNNNNN>.csv
//   run,subrun,n_events,first_event,last_event,output_file
// at the moment a file is really closed on disk (files can stay open in the
// pending-close queue after art reports them closed, see maxOpenFiles). This
// service runs in the same DataLogger process, polls that directory and writes
// for every output file:
//   <schema>.subrun_datastream  one row per subrun
//   <schema>.file_summary       one row per file (size and host from the file)
//   <schema>.file_subrun        file <-> subrun links
// A CSV row is consumed only after its file was committed to the database, so
// rows are retried on the next poll if the database is not reachable.
//
// Configuration (DataLogger art process):
//   services.Mu2eRunInfoDbService: {
//     subrunRecordDir: "/path"   # same value as RootDAQOutMF subrunRecordDir
//     datastream: "default"      # stored with every row
//     pollIntervalMs: 2000
//     outputDir: ""              # where the data files are; empty = learn it from art
//     connstr: ""                # empty = build from OTSDAQ_RUNINFO_DATABASE_* env
//     dbSchema: ""               # empty = OTSDAQ_RUNINFO_DATABASE_SCHEMA or "online"
//   }
//
// Required Postgres tables:
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

#include "TRACE/tracemf.h"

#include "art/Framework/Services/Registry/ActivityRegistry.h"
#include "art/Framework/Services/Registry/ServiceMacros.h"
#include "art/Utilities/OutputFileInfo.h"

#include "fhiclcpp/ParameterSet.h"

#include <libpq-fe.h>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <mutex>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#define TRACE_NAME "Mu2eRunInfoDbService"

namespace {

// Header written by RootDAQOutMF::writeSummaryFile(). Files with any other
// header (e.g. the CFODataReceiver subrun record) are ignored.
std::string const kCsvHeader = "run,subrun,n_events,first_event,last_event,output_file";

struct SubrunRow
{
	unsigned long long run{0};
	unsigned long long subrun{0};
	unsigned long long nEvents{0};
	unsigned long long firstEvent{0};
	unsigned long long lastEvent{0};
	std::string outputFile;
};

bool parseRow(std::string const& line, SubrunRow& row)
{
	std::vector<std::string> f;
	std::stringstream ss(line);
	std::string tok;
	while (std::getline(ss, tok, ',')) { f.push_back(tok); }
	if (f.size() != 6 || f[5].empty()) { return false; }
	try
	{
		row.run = std::stoull(f[0]);
		row.subrun = std::stoull(f[1]);
		row.nEvents = std::stoull(f[2]);
		row.firstEvent = std::stoull(f[3]);
		row.lastEvent = std::stoull(f[4]);
	}
	catch (...)
	{
		return false;
	}
	row.outputFile = f[5];
	return true;
}

std::string jsonEscape(std::string const& s)
{
	std::string out;
	for (char c : s)
	{
		if (c == '"' || c == '\\') { out += '\\'; }
		out += c;
	}
	return out;
}

}  // namespace

class Mu2eRunInfoDbService
{
public:
	Mu2eRunInfoDbService(fhicl::ParameterSet const& pset, art::ActivityRegistry& areg);
	~Mu2eRunInfoDbService();

	Mu2eRunInfoDbService(Mu2eRunInfoDbService const&) = delete;
	Mu2eRunInfoDbService& operator=(Mu2eRunInfoDbService const&) = delete;

private:
	void postCloseOutputFile_(art::OutputFileInfo const& info);
	void pollThread_();
	void scanDirectory_();
	void processCsv_(std::string const& path);
	bool ensureConnection_();
	bool writeFile_(std::vector<SubrunRow> const& rows);
	bool exec_(char const* sql);
	std::string dataDir_();

	std::string subrunRecordDir_;
	std::string datastream_;
	std::chrono::milliseconds pollInterval_;
	std::string outputDir_;
	std::string connstr_;
	std::string dbSchema_;
	std::string hostname_;

	std::mutex dirMutex_;
	std::string learnedDataDir_;  // directory of the last file art reported closed

	// only used by the poll thread
	PGconn* conn_{nullptr};
	std::map<std::string, size_t> offsets_;  // CSV path -> bytes already loaded
	std::set<std::string> ignored_;          // CSVs with an unexpected header

	std::mutex stopMutex_;
	std::condition_variable stopCv_;
	std::atomic<bool> stop_{false};
	std::thread thread_;
};

Mu2eRunInfoDbService::Mu2eRunInfoDbService(fhicl::ParameterSet const& pset, art::ActivityRegistry& areg)
	: subrunRecordDir_(pset.get<std::string>("subrunRecordDir", ""))
	, datastream_(pset.get<std::string>("datastream", "default"))
	, pollInterval_(pset.get<unsigned>("pollIntervalMs", 2000))
	, outputDir_(pset.get<std::string>("outputDir", ""))
	, connstr_(pset.get<std::string>("connstr", ""))
	, dbSchema_(pset.get<std::string>("dbSchema", ""))
{
	if (connstr_.empty())
	{
		char const* host = std::getenv("OTSDAQ_RUNINFO_DATABASE_HOST");
		char const* db = std::getenv("OTSDAQ_RUNINFO_DATABASE");
		char const* port = std::getenv("OTSDAQ_RUNINFO_DATABASE_PORT");
		char const* user = std::getenv("OTSDAQ_RUNINFO_DATABASE_USER");
		if (host && db && port && user)
		{
			std::ostringstream oss;
			oss << "host=" << host << " port=" << port << " dbname=" << db << " user=" << user;
			connstr_ = oss.str();
		}
	}
	if (dbSchema_.empty())
	{
		char const* schema_env = std::getenv("OTSDAQ_RUNINFO_DATABASE_SCHEMA");
		dbSchema_ = schema_env ? schema_env : "online";
	}

	char host[256] = {};
	gethostname(host, sizeof(host) - 1);
	hostname_ = host;

	TLOG(TLVL_INFO) << "Mu2eRunInfoDbService: subrunRecordDir=\"" << subrunRecordDir_ << "\" datastream=\""
					<< datastream_ << "\" connstr=" << (connstr_.empty() ? "(none)" : "(set)") << " schema=\""
					<< dbSchema_ << "\"";

	if (subrunRecordDir_.empty() || connstr_.empty())
	{
		TLOG(TLVL_WARNING) << "Mu2eRunInfoDbService: subrunRecordDir or DB connection not set, nothing is written to the DB";
		return;
	}

	// Rows already in the CSVs belong to earlier processes; only load what is
	// appended from now on.
	std::error_code ec;
	for (auto const& entry : std::filesystem::directory_iterator(subrunRecordDir_, ec))
	{
		if (entry.is_regular_file(ec)) { offsets_[entry.path().string()] = entry.file_size(ec); }
	}

	areg.sPostCloseOutputFile.watch(this, &Mu2eRunInfoDbService::postCloseOutputFile_);
	thread_ = std::thread([this]() { pollThread_(); });
}

Mu2eRunInfoDbService::~Mu2eRunInfoDbService()
{
	if (thread_.joinable())
	{
		{
			std::lock_guard lock(stopMutex_);
			stop_ = true;
		}
		stopCv_.notify_one();
		thread_.join();  // the thread does a last scan before it exits
	}
	if (conn_) { PQfinish(conn_); }
}

void Mu2eRunInfoDbService::postCloseOutputFile_(art::OutputFileInfo const& info)
{
	// art reports the final name when the file enters the pending-close queue;
	// the directory is all we need from it.
	std::string const& name = info.fileName();
	if (name.empty() || name == "/dev/null") { return; }
	std::lock_guard lock(dirMutex_);
	learnedDataDir_ = std::filesystem::path(name).parent_path().string();
}

std::string Mu2eRunInfoDbService::dataDir_()
{
	if (!outputDir_.empty()) { return outputDir_; }
	std::lock_guard lock(dirMutex_);
	return learnedDataDir_;
}

void Mu2eRunInfoDbService::pollThread_()
{
	while (true)
	{
		{
			std::unique_lock lock(stopMutex_);
			stopCv_.wait_for(lock, pollInterval_, [this] { return stop_.load(); });
		}
		// RootDAQOutMF writes its last rows when it is destroyed, before the
		// services, so the final scan after stop still sees them.
		scanDirectory_();
		if (stop_) { break; }
	}
	TLOG(TLVL_DEBUG) << "Mu2eRunInfoDbService poll thread exiting";
}

void Mu2eRunInfoDbService::scanDirectory_()
{
	static std::regex const kCsvName{R"(subrun_record_run\d+\.csv)"};
	std::vector<std::string> csvs;
	std::error_code ec;
	for (auto const& entry : std::filesystem::directory_iterator(subrunRecordDir_, ec))
	{
		if (entry.is_regular_file(ec) && std::regex_match(entry.path().filename().string(), kCsvName))
		{
			csvs.push_back(entry.path().string());
		}
	}
	if (ec)
	{
		TLOG(TLVL_WARNING) << "Cannot read subrunRecordDir \"" << subrunRecordDir_ << "\": " << ec.message();
		return;
	}
	std::sort(csvs.begin(), csvs.end());  // older runs first
	for (auto const& path : csvs)
	{
		if (ignored_.count(path) == 0u) { processCsv_(path); }
	}
}

void Mu2eRunInfoDbService::processCsv_(std::string const& path)
{
	size_t& offset = offsets_[path];  // new file -> 0

	int fd = open(path.c_str(), O_RDONLY);
	if (fd < 0) { return; }
	// RootDAQOutMF holds an exclusive flock while it appends one file's rows
	flock(fd, LOCK_SH);
	off_t const size = lseek(fd, 0, SEEK_END);
	std::string buf;
	if (size > static_cast<off_t>(offset))
	{
		buf.resize(static_cast<size_t>(size) - offset);
		ssize_t const n = pread(fd, buf.data(), buf.size(), static_cast<off_t>(offset));
		buf.resize(n > 0 ? static_cast<size_t>(n) : 0);
	}
	flock(fd, LOCK_UN);
	close(fd);
	if (buf.empty()) { return; }

	// Split the complete lines into consecutive groups of rows of the same
	// output file (RootDAQOutMF appends all rows of one file in one write).
	// A malformed line becomes an empty group, so it is stepped over in order.
	size_t const base = offset;
	std::vector<std::vector<SubrunRow>> groups;
	std::vector<size_t> groupEnd;  // CSV offset just after each group
	size_t pos = 0;
	while (true)
	{
		size_t const nl = buf.find('\n', pos);
		if (nl == std::string::npos) { break; }  // incomplete last line, read it next time
		std::string const line = buf.substr(pos, nl - pos);
		bool const isFirstLine = (base + pos == 0);
		pos = nl + 1;

		if (isFirstLine)
		{
			if (line != kCsvHeader)
			{
				TLOG(TLVL_WARNING) << "Ignoring " << path << ": unexpected header \"" << line << "\"";
				ignored_.insert(path);
				return;
			}
			groups.emplace_back();  // header: nothing to write
			groupEnd.push_back(base + pos);
			continue;
		}

		SubrunRow row;
		if (!parseRow(line, row))
		{
			TLOG(TLVL_WARNING) << "Skipping malformed line in " << path << ": \"" << line << "\"";
			groups.emplace_back();
			groupEnd.push_back(base + pos);
			continue;
		}
		if (groups.empty() || groups.back().empty() || groups.back().back().outputFile != row.outputFile)
		{
			groups.emplace_back();
			groupEnd.push_back(0);
		}
		groups.back().push_back(row);
		groupEnd.back() = base + pos;
	}

	for (size_t i = 0; i < groups.size(); ++i)
	{
		if (!groups[i].empty() && !writeFile_(groups[i])) { return; }  // retry from here on the next poll
		offset = groupEnd[i];
	}
}

bool Mu2eRunInfoDbService::ensureConnection_()
{
	if (conn_ && PQstatus(conn_) == CONNECTION_OK) { return true; }
	if (conn_)
	{
		PQreset(conn_);
		if (PQstatus(conn_) == CONNECTION_OK) { return true; }
		PQfinish(conn_);
		conn_ = nullptr;
	}
	conn_ = PQconnectdb(connstr_.c_str());
	if (PQstatus(conn_) != CONNECTION_OK)
	{
		TLOG(TLVL_WARNING) << "DB connection failed: " << PQerrorMessage(conn_) << " (will retry)";
		PQfinish(conn_);
		conn_ = nullptr;
		return false;
	}
	TLOG(TLVL_DEBUG) << "DB connected";
	return true;
}

bool Mu2eRunInfoDbService::exec_(char const* sql)
{
	PGresult* res = PQexec(conn_, sql);
	bool const ok = PQresultStatus(res) == PGRES_COMMAND_OK;
	PQclear(res);
	return ok;
}

// Writes the subrun rows, the file summary and the links of one output file in
// one transaction.
bool Mu2eRunInfoDbService::writeFile_(std::vector<SubrunRow> const& rows)
{
	if (!ensureConnection_()) { return false; }

	std::string const& fileName = rows.front().outputFile;
	std::string const dir = dataDir_();
	std::string const fullPath = dir.empty() ? fileName : dir + "/" + fileName;

	size_t fileSize = 0;
	std::error_code ec;
	auto const sz = std::filesystem::file_size(fullPath, ec);
	if (!ec) { fileSize = static_cast<size_t>(sz); }
	else
	{
		TLOG(TLVL_WARNING) << "Cannot stat \"" << fullPath << "\": " << ec.message() << ", file_size stored as 0";
	}

	unsigned long long nEvents = 0;
	unsigned long long firstSubrun = rows.front().subrun;
	unsigned long long lastSubrun = rows.front().subrun;
	for (auto const& r : rows)
	{
		nEvents += r.nEvents;
		firstSubrun = std::min(firstSubrun, r.subrun);
		lastSubrun = std::max(lastSubrun, r.subrun);
	}

	if (!exec_("BEGIN"))
	{
		TLOG(TLVL_WARNING) << "DB BEGIN failed: " << PQerrorMessage(conn_);
		return false;
	}

	auto fail = [&](PGresult* res, std::string const& what) {
		TLOG(TLVL_WARNING) << "DB insert failed for " << what << " of " << fileName << ": "
						   << PQresultErrorMessage(res);
		PQclear(res);
		exec_("ROLLBACK");
		return false;
	};

	std::string const subrunSql = "INSERT INTO " + dbSchema_ +
								  ".subrun_datastream "
								  "(run_number, subrun_number, n_events, first_event, last_event, datastream) "
								  "VALUES ($1,$2,$3,$4,$5,$6) RETURNING id";
	std::vector<std::string> subrunIds;
	for (auto const& r : rows)
	{
		std::string const p[6] = {std::to_string(r.run), std::to_string(r.subrun), std::to_string(r.nEvents),
								  std::to_string(r.firstEvent), std::to_string(r.lastEvent), datastream_};
		char const* params[6] = {p[0].c_str(), p[1].c_str(), p[2].c_str(), p[3].c_str(), p[4].c_str(), p[5].c_str()};
		PGresult* res = PQexecParams(conn_, subrunSql.c_str(), 6, nullptr, params, nullptr, nullptr, 0);
		if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1)
		{
			return fail(res, "subrun " + p[1]);
		}
		subrunIds.emplace_back(PQgetvalue(res, 0, 0));
		PQclear(res);
	}

	std::string const fileSql = "INSERT INTO " + dbSchema_ +
								".file_summary "
								"(file_name, run_number, first_subrun, last_subrun, n_events, file_size, "
								" metadata, datastream) "
								"VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8) RETURNING id";
	std::string const metadata = "{\"path\":\"" + jsonEscape(dir) + "\",\"hostname\":\"" + jsonEscape(hostname_) + "\"}";
	std::string const fp[8] = {fileName, std::to_string(rows.front().run), std::to_string(firstSubrun),
							   std::to_string(lastSubrun), std::to_string(nEvents), std::to_string(fileSize),
							   metadata, datastream_};
	char const* fparams[8] = {fp[0].c_str(), fp[1].c_str(), fp[2].c_str(), fp[3].c_str(),
							  fp[4].c_str(), fp[5].c_str(), fp[6].c_str(), fp[7].c_str()};
	PGresult* fres = PQexecParams(conn_, fileSql.c_str(), 8, nullptr, fparams, nullptr, nullptr, 0);
	if (PQresultStatus(fres) != PGRES_TUPLES_OK || PQntuples(fres) != 1)
	{
		return fail(fres, "file_summary");
	}
	std::string const fileId = PQgetvalue(fres, 0, 0);
	PQclear(fres);

	std::string const linkSql = "INSERT INTO " + dbSchema_ + ".file_subrun (file_summary_id, subrun_id) VALUES ($1,$2)";
	for (auto const& subrunId : subrunIds)
	{
		char const* lparams[2] = {fileId.c_str(), subrunId.c_str()};
		PGresult* res = PQexecParams(conn_, linkSql.c_str(), 2, nullptr, lparams, nullptr, nullptr, 0);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			return fail(res, "file_subrun link");
		}
		PQclear(res);
	}

	if (!exec_("COMMIT"))
	{
		TLOG(TLVL_WARNING) << "DB COMMIT failed for " << fileName << ": " << PQerrorMessage(conn_);
		exec_("ROLLBACK");
		return false;
	}
	TLOG(TLVL_DEBUG) << "Recorded " << fileName << ": subruns " << firstSubrun << "-" << lastSubrun << ", "
					 << nEvents << " events, " << fileSize << " bytes";
	return true;
}

DECLARE_ART_SERVICE(Mu2eRunInfoDbService, LEGACY)
DEFINE_ART_SERVICE(Mu2eRunInfoDbService)
