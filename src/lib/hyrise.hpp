#pragma once

#include "boost/container/pmr/memory_resource.hpp"
#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class Hyrise : public Singleton<Hyrise> {
 public:
  // Resets the Hyrise state by deleting its members (e.g., StorageManager) and
  // creating new ones. This is used especially in tests and can lead to a lot of
  // issues if there are still running tasks / threads that want to access a resource.
  // You should be very sure that this is what you want. Have a look at base_test.hpp
  // to see the correct order of resetting things.
  static void reset() { get() = Hyrise{}; }

  PluginManager plugin_manager;
  StorageManager storage_manager;
  TransactionManager transaction_manager;

 private:
  Hyrise() {
    // The default_memory_resource must be initialized before Hyrise's members so that
    // it is destructed after them and remains accessible during their deconstruction.
    // For example, when the StorageManager is destructed, it causes its stored tables
    // to be deconstructed, too. As these might call deallocate on the
    // default_memory_resource, it is important that the resource has not been
    // destructed before. As objects are destructed in the reverse order of their
    // construction, explicitly initializing the resource first means that it is
    // destructed last.
    boost::container::pmr::get_default_resource();

    plugin_manager = PluginManager{};
    storage_manager = StorageManager{};
    transaction_manager = TransactionManager{};
  }
  friend class Singleton;
};

}  // namespace opossum
