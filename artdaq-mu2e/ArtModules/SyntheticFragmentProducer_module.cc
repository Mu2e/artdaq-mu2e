// SyntheticFragmentProducer: emit synthetic artdaq::Fragments products directly
// into an art job, with no shared memory, no BoardReader and no DTC hardware.
//
// Purpose: benchmark the DataLogger output module (RootDAQOutMF) as a function of
// how many Fragment products are declared, independently of DAQ transport. Run N
// instances of this module under N distinct module labels but a SINGLE shared
// product instance name, and the output file gets N branches of the same shape the
// real DataLogger sees from N EventBuilders:
//
//   real:      artdaq::Fragments_daq_DTCEVT_eb01 ... _eb17   (N process names)
//   synthetic: artdaq::Fragments_gen01_DTCEVT_FAKEGEN ... _gen17_DTCEVT_FAKEGEN
//
// Same instance name across all N labels is what makes FragmentAggregator collapse
// them, exactly as it collapses the real per-EventBuilder fan-in.
//
// Why not reuse an existing generator: every Mu2e CommandableFragmentGenerator is a
// hardware receiver, and artdaq's GenericFragmentSimulator hardcodes
// FirstUserFragmentType (-> "MISSED"). Both are also BoardReader-side, so they drag
// in shm and force a multi-file merge whose subrun and event-ordering artifacts
// distort exactly the write-side number being measured.
//
// `stride`/`offset` reproduce the real event distribution: each event's payload
// lives in exactly ONE of the N branches (the one whose offset matches), while the
// other N-1 emit an empty vector. Those empty-but-present branches are the branch
// tax being measured. With stride=1 (default) every instance fills every event.
//
// NOTE: only payload SIZE and ENTROPY affect ROOT write speed - the fragment payload
// is serialized as an opaque block of RawDataType words, so a byte-accurate DTCEVT
// internal structure is not needed here. If a downstream overlay (DTCEventFragment)
// ever needs to parse these, a valid header would have to be written into the payload.
//
// Future: simulating a trigger (non-consecutive event ranges, i.e. a discontiguous
// RangeSet on the output) needs an EDFilter dropping events ahead of the output
// module; this producer is unchanged by that.

#include "art/Framework/Core/EDProducer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "fhiclcpp/types/Atom.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "artdaq-core/Data/Fragment.hh"

#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace mu2e {

class SyntheticFragmentProducer : public art::EDProducer
{
public:
	struct Config
	{
		fhicl::Atom<std::string> instanceName{
			fhicl::Name("instanceName"),
			fhicl::Comment("Product instance name to emit. Keep IDENTICAL across all "
						   "module labels so FragmentAggregator collapses them."),
			"DTCEVT"};
		fhicl::Atom<int> fragmentType{
			fhicl::Name("fragmentType"),
			fhicl::Comment("artdaq Fragment type_id stored in the header. Mu2e: DTCEVT=9, "
						   "CFO=12 (FIRST_USER_TYPE=1). Does not affect the branch name, "
						   "which comes from instanceName."),
			9};
		fhicl::Atom<std::size_t> payloadBytes{
			fhicl::Name("payloadBytes"),
			fhicl::Comment("Payload bytes per fragment. Production DTCEVT averages "
						   "~4.8 kB/event/EventBuilder."),
			4800};
		fhicl::Atom<std::size_t> fragmentsPerEvent{
			fhicl::Name("fragmentsPerEvent"),
			fhicl::Comment("Fragments in the vector on events this instance fills"),
			1};
		fhicl::Atom<std::size_t> stride{
			fhicl::Name("stride"),
			fhicl::Comment("Fill only events where (event % stride) == offset; emit an "
						   "empty vector otherwise. Set to the number of producer "
						   "instances to reproduce the real one-EventBuilder-per-event "
						   "pattern. 1 = fill every event."),
			1};
		fhicl::Atom<std::size_t> offset{
			fhicl::Name("offset"),
			fhicl::Comment("This instance's slot in the stride, 0 <= offset < stride"),
			0};
		fhicl::Atom<std::string> fillMode{
			fhicl::Name("fillMode"),
			fhicl::Comment("zero = all zeros (compresses away, isolates branch overhead); "
						   "random = incompressible, representative of real compressed size"),
			"random"};
		fhicl::Atom<unsigned> seed{
			fhicl::Name("seed"),
			fhicl::Comment("PRNG seed for the fillMode:random payload pool"),
			20260906};
		fhicl::Atom<std::size_t> poolBytes{
			fhicl::Name("poolBytes"),
			fhicl::Comment("Size of the random payload pool, bytes. Each fragment copies a "
						   "different window out of it, so this must be MUCH larger than "
						   "payloadBytes or consecutive events share most of their bytes and "
						   "ROOT's per-basket compression collapses them - which silently "
						   "overstates write throughput and understates file size."),
			8388608};
		fhicl::Atom<int> fragmentId{
			fhicl::Name("fragmentId"),
			fhicl::Comment("Base fragment_id written into the fragment header"),
			0};
	};

	using Parameters = art::EDProducer::Table<Config>;

	explicit SyntheticFragmentProducer(Parameters const& params);

	void produce(art::Event& event) override;

private:
	std::string instanceName_;
	artdaq::Fragment::type_t fragmentType_;
	std::size_t payloadBytes_;
	std::size_t fragmentsPerEvent_;
	std::size_t stride_;
	std::size_t offset_;
	int fragmentId_;

	// Payload source. Built once at construction so produce() is a memcpy: the
	// generator must stay cheap relative to the output module it is measuring.
	// It is deliberately much larger than one payload, and each fragment copies
	// from a different position, so consecutive events do NOT carry identical
	// bytes - identical payloads would let ROOT's per-basket compression collapse
	// many events into almost nothing and badly overstate write throughput.
	std::vector<uint8_t> pool_;
	std::size_t poolSpan_;  // number of distinct start positions in pool_
};

}  // namespace mu2e

mu2e::SyntheticFragmentProducer::SyntheticFragmentProducer(Parameters const& params)
	: art::EDProducer{params}, instanceName_(params().instanceName()), fragmentType_(static_cast<artdaq::Fragment::type_t>(params().fragmentType())), payloadBytes_(params().payloadBytes()), fragmentsPerEvent_(params().fragmentsPerEvent()), stride_(params().stride()), offset_(params().offset()), fragmentId_(params().fragmentId())
{
	if (stride_ == 0)
	{
		throw art::Exception(art::errors::Configuration)
			<< "SyntheticFragmentProducer: stride must be >= 1";
	}
	if (offset_ >= stride_)
	{
		throw art::Exception(art::errors::Configuration)
			<< "SyntheticFragmentProducer: offset (" << offset_
			<< ") must be < stride (" << stride_ << ")";
	}
	if (payloadBytes_ == 0)
	{
		throw art::Exception(art::errors::Configuration)
			<< "SyntheticFragmentProducer: payloadBytes must be >= 1";
	}

	auto const& mode = params().fillMode();
	if (mode != "zero" && mode != "random")
	{
		throw art::Exception(art::errors::Configuration)
			<< "SyntheticFragmentProducer: fillMode must be 'zero' or 'random', got '"
			<< mode << "'";
	}

	// Distinct start positions available to consecutive fragments. Must be large
	// relative to payloadBytes_ so that windows overlap little and the written data
	// stays as incompressible as the fill mode intends.
	poolSpan_ = params().poolBytes();
	if (poolSpan_ < payloadBytes_)
	{
		poolSpan_ = payloadBytes_;
	}
	pool_.assign(payloadBytes_ + poolSpan_, 0);
	if (mode == "random")
	{
		std::mt19937_64 rng(params().seed());
		std::size_t const words = pool_.size() / sizeof(uint64_t);
		auto* p = reinterpret_cast<uint64_t*>(pool_.data());
		for (std::size_t i = 0; i < words; ++i)
		{
			p[i] = rng();
		}
		for (std::size_t i = words * sizeof(uint64_t); i < pool_.size(); ++i)
		{
			pool_[i] = static_cast<uint8_t>(rng());
		}
	}

	produces<artdaq::Fragments>(instanceName_);

	mf::LogInfo("SyntheticFragmentProducer")
		<< "instance=" << instanceName_ << " type=" << static_cast<int>(fragmentType_)
		<< " payloadBytes=" << payloadBytes_ << " fragsPerEvent=" << fragmentsPerEvent_
		<< " stride=" << stride_ << " offset=" << offset_ << " fill=" << mode
		<< " poolBytes=" << poolSpan_;
}

void mu2e::SyntheticFragmentProducer::produce(art::Event& event)
{
	auto frags = std::make_unique<artdaq::Fragments>();

	auto const evtNum = static_cast<std::size_t>(event.event());

	// Events not owned by this instance still get a product - an EMPTY one. That is
	// the real DataLogger's situation: the branch exists and is written on every
	// event, but holds data only on the events its EventBuilder contributed.
	if (evtNum % stride_ == offset_)
	{
		frags->reserve(fragmentsPerEvent_);
		for (std::size_t i = 0; i < fragmentsPerEvent_; ++i)
		{
			artdaq::Fragment frag(
				static_cast<artdaq::Fragment::sequence_id_t>(evtNum),
				static_cast<artdaq::Fragment::fragment_id_t>(fragmentId_ + i),
				fragmentType_,
				static_cast<artdaq::Fragment::timestamp_t>(evtNum));
			frag.resizeBytes(payloadBytes_);

			// Deterministic per (event, fragment) start position - no mutable module
			// state, so this stays correct if art schedules events concurrently.
			std::size_t const start = (evtNum * 7919 + i * 104729) % poolSpan_;
			std::memcpy(frag.dataBeginBytes(), pool_.data() + start, payloadBytes_);

			frags->emplace_back(std::move(frag));
		}
	}

	event.put(std::move(frags), instanceName_);
}

DEFINE_ART_MODULE(mu2e::SyntheticFragmentProducer)
