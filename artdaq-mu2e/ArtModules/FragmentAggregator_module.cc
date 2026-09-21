#include "art/Framework/Core/EDProducer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/Selector.h"
#include "fhiclcpp/types/Sequence.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "artdaq-core/Data/Fragment.hh"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace mu2e {

class FragmentAggregator : public art::EDProducer
{
public:
	struct Config
	{
		fhicl::Sequence<std::string> instanceNames{
			fhicl::Name("instanceNames"),
			fhicl::Comment("Fragment product instance names to aggregate and re-emit.\n"
						   "Only names that actually carry data belong here: each one costs a "
						   "getMany query that materialises its branches in every event. "
						   "ContainerDTCEVT was dropped from this default after measurement - "
						   "in production files it sits exactly at the empty-branch floor "
						   "(240,844 bytes/branch across 17 EventBuilders, i.e. no payload), "
						   "whereas CFO is ~1.16 MB above it."),
			std::vector<std::string>{"CFO", "DTCEVT"}};
		fhicl::Atom<bool> useInstanceSelector{
			fhicl::Name("useInstanceSelector"),
			fhicl::Comment("true (default): retrieve only the configured instance names, via "
						   "one getMany(ProductInstanceNameSelector) per name. Selectors match "
						   "on BranchDescription metadata, so non-matching products are never "
						   "resolved - unrelated Fragment branches (TRK, CAL, CRV, STM, DBG ... "
						   "which are empty in Mu2e running) cost nothing.\n"
						   "false: legacy bare getMany<Fragments>(), which retrieves and "
						   "MATERIALISES every Fragments product in the event and only then "
						   "filters by instance name - measured at ~5 us per unwanted branch "
						   "per event. Kept for A/B measurement only."),
			true};
		fhicl::Atom<bool> throwOnDuplicate{
			fhicl::Name("throwOnDuplicate"),
			fhicl::Comment("Throw if two input handles share an instance name (false = concatenate)"),
			true};
	};

	using Parameters = art::EDProducer::Table<Config>;

	explicit FragmentAggregator(Parameters const& params);

	void produce(art::Event& event) override;

private:
	std::set<std::string> instanceNames_;
	bool useInstanceSelector_;
	bool throwOnDuplicate_;
};

}  // namespace mu2e

mu2e::FragmentAggregator::FragmentAggregator(Parameters const& params)
	: art::EDProducer{params}, useInstanceSelector_(params().useInstanceSelector()), throwOnDuplicate_(params().throwOnDuplicate())
{
	auto const& names = params().instanceNames();
	for (auto const& name : names)
	{
		instanceNames_.insert(name);
		produces<artdaq::Fragments>(name);
	}

	consumesMany<artdaq::Fragments>();
}

void mu2e::FragmentAggregator::produce(art::Event& event)
{
	std::map<std::string, std::unique_ptr<artdaq::Fragments>> aggregated;
	for (auto const& name : instanceNames_)
	{
		aggregated[name] = std::make_unique<artdaq::Fragments>();
	}

	std::map<std::string, std::string> seenSources;

	// Merge one retrieved handle into its instance's output vector. Shared by both
	// retrieval paths so they stay behaviourally identical.
	auto absorb = [&](art::Handle<artdaq::Fragments> const& handle) {
		if (!handle.isValid() || handle->empty())
			return;

		std::string const& instance = handle.provenance()->productInstanceName();

		if (instanceNames_.find(instance) == instanceNames_.end())
			return;

		auto it = seenSources.find(instance);
		if (it != seenSources.end())
		{
			if (throwOnDuplicate_)
			{
				throw art::Exception(art::errors::DataCorruption)
					<< "FragmentAggregator: duplicate instance name '" << instance
					<< "' -- first from process '" << it->second
					<< "', again from '" << handle.provenance()->processName()
					<< "' (set throwOnDuplicate: false to concatenate instead)";
			}
		}
		else
		{
			seenSources[instance] = handle.provenance()->processName();
		}

		auto& dest = *aggregated[instance];
		dest.reserve(dest.size() + handle->size());
		for (auto const& frag : *handle)
		{
			dest.push_back(frag);
		}
	};

	if (useInstanceSelector_)
	{
		// One query per wanted instance name. Products whose BranchDescription does
		// not match are filtered on metadata and never read off disk.
		for (auto const& name : instanceNames_)
		{
			for (auto const& handle :
				 event.getMany<artdaq::Fragments>(art::ProductInstanceNameSelector{name}))
			{
				absorb(handle);
			}
		}
	}
	else
	{
		for (auto const& handle : event.getMany<artdaq::Fragments>())
		{
			absorb(handle);
		}
	}

	for (auto& [name, frags] : aggregated)
	{
		event.put(std::move(frags), name);
	}
}

DEFINE_ART_MODULE(mu2e::FragmentAggregator)
