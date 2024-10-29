#include <utility>
#include <string>

#include "pdgf_process.hpp"
#include "utils/assert.hpp"


namespace hyrise {

PdgfProcess PdgfProcess::for_schema_generation(std::string pdgf_directory_root) {
  return PdgfProcess(std::move(pdgf_directory_root), "-writeTableSchemas");
}

PdgfProcess PdgfProcess::for_data_generation(std::string pdgf_directory_root) {
  return PdgfProcess(std::move(pdgf_directory_root), "-start");
}

PdgfProcess::PdgfProcess(std::string pdgf_directory_root, std::string pdgf_command)
    : _pdgf_directory_root(std::move(pdgf_directory_root)), _pdgf_command(std::move(pdgf_command)) {}

PdgfProcess::~PdgfProcess() {
  std::cerr << "Destructuring PdgfProcess!\n";
  for (auto& thread: _reader_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  // We don't want to wait until this thread wakes up again
  _monitor_thread.detach();
  if (_child.running()) {
    _child.terminate();
  }
  if (_child_out.is_open()) {
    _child_out.close();
  }
  if (_child_err.is_open()) {
    _child_err.close();
  }
}

void PdgfProcess::run() {
  Assert(!_has_run, "Each pdgf process instance should only be run once!");
  _has_run = true;

  // Setup arguments
  _configure_numa();
  _arguments.emplace_back("/usr/lib/jvm/java-8-openjdk/bin/java");
  _configure_jvm();
  _configure_pdgf_properties();
  _arguments.emplace_back("-jar");
  _arguments.emplace_back("pdgf_patched.jar");
  _configure_pdgf_arguments();

  // Argument info
  std::cout << "Executing PDGF!\n";
  std::cout << "/usr/bin/numactl ";
  for (const auto& arg: _arguments) {
    std::cout << arg << " ";
  }
  std::cout << "\n";

  // Input reading
  _reader_threads.emplace_back([&] {
    std::string line;
    while (std::getline(_child_out, line)) {
      std::cout << "[PDGF] " << line << "\n";
    }
  });
  _reader_threads.emplace_back([&] {
    std::string line;
    while (std::getline(_child_err, line)) {
      std::cout << "[PDGF ERR] " << line << "\n";
    }
  });

  // Run
  _child = boost::process::child(
      "/usr/bin/numactl",
      boost::process::args(_arguments),
      boost::process::start_dir(_pdgf_directory_root),
      boost::process::std_out > _child_out,
      boost::process::std_err > _child_err);

  // This is just a fail-safe, so that we won't block forever in benchmarks waiting for data when PDGF has crashed.
  _monitor_thread = std::thread([&] {
    while (true) {
      if (!_child.running()) {
        if (!_data_transmission_complete) {
          std::cerr << "Child finished before data transmission was complete!\n";
          exit(EXIT_FAILURE);
        }
        break;
      }
      // We need to wait sufficiently long here, because PDGF will already terminate once it has managed to write
      // all the data, while Hyrise still has some work to do reading the remaining cells.
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
  });
}

void PdgfProcess::await_teardown() {
  _data_transmission_complete = true;
  _child.wait();
  std::cout << _child.exit_code() << "\n";
}

void PdgfProcess::set_column_filter(std::shared_ptr<std::set<std::string>>& columns_to_generate) {
  _columns_to_generate = columns_to_generate;
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
  if (_columns_to_generate && !_columns_to_generate->empty()) {
    _arguments.emplace_back("-filterTableFields");
    std::copy(_columns_to_generate->begin(), _columns_to_generate->end(), std::back_inserter(_arguments));
  }
  _arguments.insert(_arguments.end(), {
                                          "-load", "pdgf-core_config_tpc-h-schema.xml",
                                          "-load", "default-shm-reflective-generation.xml",
                                          "-noShell", "-closeWhenDone",
                                          "-sf", "0.1",
                                          "-workers", "1",
                                          _pdgf_command
                                      });
}
}  // namespace hyrise