cet_make_library(LIBRARY_NAME Mu2eReceiverBase
SOURCE
Mu2eEventReceiverBase.cc
LIBRARIES PUBLIC
artdaq::DAQdata
canvas::canvas
artdaq-core-mu2e::Overlays
mu2e-pcie-utils::DTCInterface
artdaq_plugin_types::CommandableFragmentGenerator
  )

include(artdaq::commandableGenerator)

cet_build_plugin(Mu2eEventReceiver artdaq::commandableGenerator LIBRARIES REG artdaq-mu2e::Mu2eReceiverBase
  )

cet_build_plugin(CFODataReceiver artdaq::commandableGenerator
  LIBRARIES REG
  artdaq-core-mu2e::Overlays
  mu2e-pcie-utils::DTCInterface
  mu2e-pcie-utils::CFOInterface
  artdaq_plugin_types::CommandableFragmentGenerator
  PostgreSQL::PostgreSQL
)

cet_build_plugin(CFOZmqReceiver artdaq::commandableGenerator
  LIBRARIES REG
  artdaq-core-mu2e::Overlays
  cppzmq
  artdaq_plugin_types::CommandableFragmentGenerator
)

cet_build_plugin(Mu2eSubEventReceiver artdaq::commandableGenerator
  LIBRARIES REG
  artdaq-core-mu2e::Overlays
  mu2e-pcie-utils::DTCInterface
  artdaq_plugin_types::CommandableFragmentGenerator
)

cet_build_plugin(CRVReceiver artdaq::commandableGenerator LIBRARIES REG artdaq-mu2e::Mu2eReceiverBase)

# Software-only generator for DAQ throughput tests - no DTC. Needs only the Mu2e type map.
cet_build_plugin(Mu2eFragmentSimulator artdaq::commandableGenerator
  LIBRARIES REG
  artdaq-core-mu2e::Overlays
  artdaq_plugin_types::CommandableFragmentGenerator
)

#get_cmake_property(_variableNames VARIABLES)
#foreach (_variableName ${_variableNames})
#    message(STATUS "${_variableName}=${${_variableName}}")
#endforeach()

install_headers()
install_source()
