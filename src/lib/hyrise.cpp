#include "hyrise.hpp"

namespace opossum {

Hyrise::Hyrise()
    : plugin_manager{PluginManager::get()},
      storage_manager{StorageManager::get()},
      transaction_manager{TransactionManager::get()} {}

void Hyrise::reset() {
  auto& hyrise = get();
  hyrise.plugin_manager.reset();
  hyrise.storage_manager.reset();
  hyrise.transaction_manager.reset();
}

}  // namespace opossum
