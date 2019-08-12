#pragma once

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
};

}  // namespace opossum
