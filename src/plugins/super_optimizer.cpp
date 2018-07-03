// Usually, this should be in a separate folder. For simplicity, I put it in ./src/plugins for now.

#include <functional>
#include <thread>
#include "storage/storage_manager.hpp"

// For now, I did not touch the cmake files. On OS X, build this in ./plugins using
// clang++ -dynamiclib ../src/plugins/super_optimizer.cpp -I../src/lib -undefined dynamic_lookup -fvisibility=hidden -o super_optimizer.dylib

class SuperOptimizer {
  std::thread super_thread;
  static SuperOptimizer* instance;

 public:
  static void start() {
    if (instance) delete instance;
    instance = new SuperOptimizer();
  }

  static void stop() { delete instance; }

 protected:
  SuperOptimizer() {
    _run = true;
    super_thread = std::thread(std::bind(&SuperOptimizer::_check_table_count, this));
  }

  ~SuperOptimizer() {
    _run = false;
    super_thread.join();
  }

  bool _run = false;

  void _check_table_count() {
    while (_run) {
      using namespace std::chrono_literals;
      auto table_names = opossum::StorageManager::get().table_names();
      if (table_names.size() > 5) {
        std::cerr << std::endl << "... reducing database footprint ..." << std::endl;
        auto table_it = table_names.begin();
        std::advance(table_it, std::rand() % table_names.size());
        opossum::StorageManager::get().drop_table(*table_it);
      }
      std::this_thread::sleep_for(10s);
    }
  }
};

SuperOptimizer* SuperOptimizer::instance;

__attribute__((constructor)) void init() { SuperOptimizer::start(); }

__attribute__((destructor)) void unload() { SuperOptimizer::stop(); }

extern "C" __attribute__((visibility("default"))) const char* name() {
  // since this is C linkage, we cannot return std::string
  // we could use C++ linkage, but then we have ugly mangled names everywhere
  return "Markus' SuperOptimizer";
}