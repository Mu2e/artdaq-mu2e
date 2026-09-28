// Mu2eFragmentSimulator: a BoardReader fragment generator that synthesises Mu2e
// Fragments in software - no DTC, no CFO, no hardware of any kind.
//
// Why this exists: every other Mu2e generator (Mu2eEventReceiver, CFODataReceiver,
// Mu2eSubEventReceiver, CRVReceiver) is a hardware receiver, so the DAQ chain cannot be
// exercised at all without a DTC. The only software alternative was artdaq-demo's ToySim,
// which hardcodes TOY1/TOY2/TOY21 through the DEMO type map and therefore cannot produce
// Mu2e instance names. That matters more than cosmetics: the branch names at the
// DataLogger, and the set of instance names each EventBuilder declares, come from the
// fragment TYPE via the Mu2e FragmentNameHelper. With ToySim everything lands in
// "unidentified"; with this generator the DataLogger sees real
// artdaq::Fragments_daq_DTCEVT_<ebNN> branches and the full 25-name declared list.
//
// This is the BoardReader-side twin of SyntheticFragmentProducer (an art EDProducer used
// for the standalone output-module benchmark). Same idea, opposite end of the chain: this
// one injects through shared memory so the whole BR -> EB -> DataLogger path is real.
//
// Typical use - one BoardReader per fragment type, which is the production event shape:
//   br01: { generator: Mu2eFragmentSimulator fragment_type: "DTCEVT" payload_bytes: 4915 }
//   br02: { generator: Mu2eFragmentSimulator fragment_type: "CFO"    payload_bytes: 600  }
//
// NOTE: only payload SIZE and ENTROPY affect transport and ROOT write speed - the payload
// is an opaque block of RawDataType words. A byte-accurate DTCEvent header is NOT written,
// so anything that tries to PARSE these with DTCEventFragment (an overlay, a dump module)
// will fail. This is a throughput generator, not a data-format simulator.

#include "artdaq-core-mu2e/Overlays/FragmentType.hh"
#include "artdaq/Generators/CommandableFragmentGenerator.hh"
#include "artdaq/Generators/GeneratorMacros.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/Globals.hh"

#include "fhiclcpp/ParameterSet.h"
#include "cetlib_except/exception.h"

#define TRACE_NAME "Mu2eFragmentSimulator"
#include "TRACE/tracemf.h"

#include <unistd.h>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace mu2e {

class Mu2eFragmentSimulator : public artdaq::CommandableFragmentGenerator
{
public:
	explicit Mu2eFragmentSimulator(fhicl::ParameterSet const& ps);
	~Mu2eFragmentSimulator() override = default;

private:
	bool getNext_(artdaq::FragmentPtrs& frags) override;
	void start() override { timestamp_ = 0; }
	void stop() override {}
	void stopNoMutex() override {}

	artdaq::Fragment::type_t fragmentType_;
	std::size_t payloadBytes_;
	std::size_t fragmentsPerEvent_;
	std::size_t throttleUsecs_;
	artdaq::Fragment::timestamp_t timestamp_;

	// Payload source, built once at construction so getNext_ is a memcpy. Deliberately
	// much larger than one payload, with each fragment copying from a different offset:
	// identical payloads would let ROOT's per-basket compression collapse many events
	// into almost nothing, silently overstating write throughput downstream.
	std::vector<uint8_t> pool_;
	std::size_t poolSpan_;
};

}  // namespace mu2e

mu2e::Mu2eFragmentSimulator::Mu2eFragmentSimulator(fhicl::ParameterSet const& ps)
	: artdaq::CommandableFragmentGenerator(ps)
	, payloadBytes_(ps.get<std::size_t>("payload_bytes", 4915))
	, fragmentsPerEvent_(ps.get<std::size_t>("fragments_per_event", 1))
	, throttleUsecs_(ps.get<std::size_t>("throttle_usecs", 0))
	, timestamp_(0)
{
	auto typeName = ps.get<std::string>("fragment_type", "DTCEVT");
	auto ft = mu2e::toFragmentType(typeName);
	if (ft == mu2e::FragmentType::INVALID)
	{
		throw cet::exception("Mu2eFragmentSimulator")  // NOLINT(cert-err60-cpp)
			<< "unknown fragment_type \"" << typeName
			<< "\" - must be one of the names in artdaq-core-mu2e's FragmentTypeMap "
			<< "(DTCEVT, CFO, TRK, CAL, CRV, STM, TRKDTC, ...)";
	}
	fragmentType_ = static_cast<artdaq::Fragment::type_t>(ft);

	if (payloadBytes_ == 0)
	{
		throw cet::exception("Mu2eFragmentSimulator") << "payload_bytes must be >= 1";  // NOLINT(cert-err60-cpp)
	}

	auto fillMode = ps.get<std::string>("fill_mode", "random");
	if (fillMode != "random" && fillMode != "zero")
	{
		throw cet::exception("Mu2eFragmentSimulator")  // NOLINT(cert-err60-cpp)
			<< "fill_mode must be \"random\" or \"zero\", got \"" << fillMode << "\"";
	}

	poolSpan_ = ps.get<std::size_t>("pool_bytes", 8388608);
	if (poolSpan_ < payloadBytes_) { poolSpan_ = payloadBytes_; }
	pool_.assign(payloadBytes_ + poolSpan_, 0);
	if (fillMode == "random")
	{
		std::mt19937_64 rng(ps.get<unsigned>("seed", 20260909));
		std::size_t const words = pool_.size() / sizeof(uint64_t);
		auto* p = reinterpret_cast<uint64_t*>(pool_.data());
		for (std::size_t i = 0; i < words; ++i) { p[i] = rng(); }
		for (std::size_t i = words * sizeof(uint64_t); i < pool_.size(); ++i)
		{
			pool_[i] = static_cast<uint8_t>(rng());
		}
	}

	TLOG(TLVL_INFO) << "Mu2eFragmentSimulator: type=" << typeName << " (" << static_cast<int>(fragmentType_)
	                << ") payload_bytes=" << payloadBytes_ << " frags_per_event=" << fragmentsPerEvent_
	                << " fill=" << fillMode << " pool_bytes=" << poolSpan_
	                << " throttle_usecs=" << throttleUsecs_;
}

bool mu2e::Mu2eFragmentSimulator::getNext_(artdaq::FragmentPtrs& frags)
{
	if (should_stop()) { return false; }
	if (throttleUsecs_ > 0) { usleep(throttleUsecs_); }

	auto const seq = static_cast<artdaq::Fragment::sequence_id_t>(ev_counter());

	for (auto& id : fragmentIDs())
	{
		for (std::size_t i = 0; i < fragmentsPerEvent_; ++i)
		{
			frags.emplace_back(artdaq::Fragment::FragmentBytes(
				payloadBytes_, seq, static_cast<artdaq::Fragment::fragment_id_t>(id),
				fragmentType_, timestamp_));

			// Deterministic per (event, fragment) offset - no mutable state, so the
			// payload stays incompressible without depending on call order.
			std::size_t const start = (static_cast<std::size_t>(seq) * 7919 + i * 104729) % poolSpan_;
			std::memcpy(frags.back()->dataBeginBytes(), pool_.data() + start, payloadBytes_);
		}
	}

	if (metricMan != nullptr)
	{
		metricMan->sendMetric("Fragments Sent", ev_counter(), "Events", 3, artdaq::MetricMode::LastPoint);
	}

	++timestamp_;
	ev_counter_inc();
	return true;
}

DEFINE_ARTDAQ_COMMANDABLE_GENERATOR(mu2e::Mu2eFragmentSimulator)
