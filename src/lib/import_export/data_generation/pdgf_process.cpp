#include "pdgf_process.hpp"

#include <utility>

namespace hyrise {

PdgfProcess::PdgfProcess(std::string pdgf_directory_root) : _pdgf_directory_root(std::move(pdgf_directory_root)) {
  _configure_numa();
  _arguments.emplace_back("/usr/lib/jvm/java-8-openjdk/bin/java");
  _configure_jvm();
  _configure_pdgf_properties();
  _arguments.emplace_back("-jar");
  _arguments.emplace_back("pdgf_patched.jar");
  _configure_pdgf_arguments();
}

void PdgfProcess::run() {
  std::cout << "Executing PDGF!\n";
  std::cout << "/usr/bin/numactl ";
  for (const auto& arg: _arguments) {
    std::cout << arg << " ";
  }
  std::cout << "\n";

  _child = boost::process::child(
      "/usr/bin/numactl",
      boost::process::args(_arguments),
      boost::process::start_dir(_pdgf_directory_root));
}

void PdgfProcess::wait() {
  _child.wait();
  std::cout << _child.exit_code() << "\n";
}

void PdgfProcess::_configure_numa() {
  _arguments.insert(_arguments.end(), {"-N", "0", "-m", "0"});
}

void PdgfProcess::_configure_jvm() {
  _arguments.insert(_arguments.end(), {"-Xms20g", "-Xmx20g", "-XX:TLABSize=4000k"});
}

void PdgfProcess::_configure_pdgf_properties() {
  auto properties = std::vector<std::string>{
      "java.library.path", "extlib/",
      "bankmark.pdgf.log.folder", "/scratch/jan-eric.hellenberg",
      "CONCURRENT_SCHED_DEFAULT_WORKUNIT_SIZE", "128"
  };

  for (size_t i = 0; i < properties.size(); i += 2) {
    auto arg = std::string{};
    arg.append("-D").append(properties[i]).append("=").append(properties[i + 1]);
    _arguments.emplace_back(arg);
  }
}

void PdgfProcess::_configure_pdgf_arguments() {
  _arguments.insert(_arguments.end(), {
                                          "-load", "pdgf-core_config_tpc-h-schema.xml",
                                          "-load", "default-shm-reflective-generation.xml",
                                          "-noShell", "-closeWhenDone",
                                          "-sf", "1",
                                          "-workers", "1",
                                          "-start"
                                      });
}
}  // namespace hyrise