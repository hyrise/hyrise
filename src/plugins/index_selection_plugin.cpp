#include "index_selection_plugin.hpp"

#include "operators/get_table.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/base_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"

namespace opossum {

const std::string IndexSelectionPlugin::description() const { return "IndexSelectionPlugin"; }

void IndexSelectionPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!");
}

void IndexSelectionPlugin::stop() {}


EXPORT_PLUGIN(IndexSelectionPlugin)

}  // namespace opossum