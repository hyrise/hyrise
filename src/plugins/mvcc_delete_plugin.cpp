#include "mvcc_delete_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "This is the Hyrise TestPlugin"; }

void MvccDeletePlugin::start() {

}

void MvccDeletePlugin::stop() { StorageManager::get().drop_table("DummyTable"); }

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum

