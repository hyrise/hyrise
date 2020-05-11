#include "index_selection_plugin.hpp"

namespace opossum {

const std::string IndexSelectionPlugin::description() const { return "IndexSelectionPlugin"; }

void IndexSelectionPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!", LogLevel::Info);
}

void IndexSelectionPlugin::stop() {}


EXPORT_PLUGIN(IndexSelectionPlugin)

}  // namespace opossum