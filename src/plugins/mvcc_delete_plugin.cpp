#include "mvcc_delete_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "This is the Hyrise TestPlugin"; }

void MvccDeletePlugin::start() {
  //TODO: Implement MVCC Delete logic
}

void MvccDeletePlugin::stop() {
  //TODO: Implement if necessary
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum

