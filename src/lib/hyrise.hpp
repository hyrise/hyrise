#pragma once

#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class Hyrise : public Singleton<Hyrise> {
 public:
  static void reset();

  PluginManager plugin_manager;
  StorageManager storage_manager;
  TransactionManager transaction_manager;

 private:
  Hyrise();
  friend class Singleton;

  //const Hyrise& operator=(const Hyrise&) = delete;
  //Hyrise& operator=(Hyrise&&) = default;
};

}  // namespace opossum
