#include "compression_plugin.hpp"

#include "operators/get_table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "storage/base_segment.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"

namespace opossum {

const std::string CompressionPlugin::description() const { return "CompressionPlugin"; }

void CompressionPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!");
  _memory_budget_setting = std::make_shared<MemoryBudgetSetting>();
  _memory_budget_setting->register_at_settings_manager();
  _loop_thread = std::make_unique<PausableLoopThread>(THREAD_INTERVAL, [&](size_t) { _optimize_compression(); });
}

void CompressionPlugin::_optimize_compression() {
  if (_memory_budget_setting->get() == "5000") {
    Hyrise::get().log_manager.add_message(description(), "Target memory budget sufficient, not optimizing.");
    return;
  }

  if (_optimized) return;

  _optimized = true;

  if (!Hyrise::get().storage_manager.has_table("lineitem")) {
    Hyrise::get().log_manager.add_message(description(), "No optimization possible with given parameters!");
    return;
  }
  auto table = Hyrise::get().storage_manager.get_table("lineitem");

  const auto column_id = ColumnID{15};  //l_comment
  if (table->column_count() <= static_cast<ColumnCount>(column_id)) {
    Hyrise::get().log_manager.add_message(description(), "No optimization possible with given parameters!");
    return;
  }

  const auto data_type = table->column_data_type(column_id);

  size_t memory_usage_old = 0;
  size_t memory_usage_new = 0;

  for (ChunkID chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    const auto base_segment = chunk->get_segment(column_id);
    memory_usage_old += base_segment->memory_usage(MemoryUsageCalculationMode::Sampled);

    std::shared_ptr<BaseSegment> new_segment;
    new_segment = encode_and_compress_segment(base_segment, data_type, SegmentEncodingSpec{EncodingType::LZ4});
    memory_usage_new += new_segment->memory_usage(MemoryUsageCalculationMode::Sampled);

    chunk->replace_segment(column_id, new_segment);
  }

  auto mb_saved = (memory_usage_old - memory_usage_new) / 1'000'000;
  Hyrise::get().log_manager.add_message(
      description(), "Applied new compression configuration - saved " + std::to_string(mb_saved) + "MB.");
}

void CompressionPlugin::stop() { _memory_budget_setting->unregister_at_settings_manager(); }

EXPORT_PLUGIN(CompressionPlugin)

}  // namespace opossum
