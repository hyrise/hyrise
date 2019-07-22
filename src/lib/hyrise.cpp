#include "hyrise.hpp"

namespace opossum {

Hyrise::Hyrise()
    : plugin_manager(PluginManager{}), storage_manager(StorageManager{}), transaction_manager(TransactionManager{}) {}

void Hyrise::reset() { get() = Hyrise{}; }

}  // namespace opossum
