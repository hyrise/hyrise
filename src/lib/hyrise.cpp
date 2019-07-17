#include "hyrise.hpp"

namespace opossum {

Hyrise::Hyrise() : plugin_manager(PluginManager{}),
                   storage_manager(StorageManager{}),
                   transaction_manager(TransactionManager{}) {}

void Hyrise::reset() {
  //get() = Hyrise{};
  auto& hyrise = Hyrise::get();
  hyrise.plugin_manager = PluginManager{};
  hyrise.storage_manager = StorageManager{};
  hyrise.transaction_manager = TransactionManager{};
}

}  // namespace opossum
