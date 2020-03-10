#include "compression_plugin.hpp"

#include "operators/get_table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/base_segment.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "resolve_type.hpp"

namespace opossum {

const std::string CompressionPlugin::description() const { return "CompressionPlugin"; }

void CompressionPlugin::start() {
  Hyrise::get().log_manager.add_message(description(), "Initialized!");
  _loop_thread = std::make_unique<PausableLoopThread>(THREAD_INTERVAL, [&](size_t) { _optimize_compression(); });
}

void CompressionPlugin::_optimize_compression() {
  if (_optimized) return;

  _optimized = true;

  auto table = Hyrise::get().storage_manager.get_table("lineitem");

  const auto column_id = ColumnID{15}; //l_comment

  const auto data_type = table->column_data_type(column_id);

  for (ChunkID chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    const auto base_segment = chunk->get_segment(column_id);
    std::shared_ptr<BaseSegment> new_segment;
    new_segment = encode_and_compress_segment(base_segment, data_type, SegmentEncodingSpec{EncodingType::LZ4});
    chunk->replace_segment(column_id, new_segment);
  }

  Hyrise::get().log_manager.add_message(description(), "Optimization completed!");
}

void CompressionPlugin::stop() {}


EXPORT_PLUGIN(CompressionPlugin)

}  // namespace opossum
