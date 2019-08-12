#pragma once

#include "boost/container/pmr/memory_resource.hpp"
#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class Hyrise : public Singleton<Hyrise> {
 public:
  // Resets the Hyrise state by deleting the Plugin-, Storage-, and TransactionManager
  // and creating new ones. This is used especially in tests and can lead to a lot of
  // issues if there are still running tasks / threads that want to access a resource.
  // You should be very sure that this is what you want. Have a look at base_test.hpp
  // to see the correct order of resetting things.
  static void reset() { get() = Hyrise{}; }

  PluginManager plugin_manager;
  StorageManager storage_manager;
  TransactionManager transaction_manager;

 private:
  Hyrise() {
    // The default_memory_resource must be initialized before Plugin-, Storage-, and
    // TransactionManager so that it is the last object to be destructed. This prevents
    // e.g. the StorageManager destructor to deallocate a table that is already
    // destructed by the default_memory_resource destructor.
    boost::container::pmr::get_default_resource();

    plugin_manager = PluginManager{};
    storage_manager = StorageManager{};
    transaction_manager = TransactionManager{};
  }
  friend class Singleton;
};

}  // namespace opossum
