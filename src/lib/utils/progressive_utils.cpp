#include "progressive_utils.hpp"

#include <memory>

#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace hyrise {

namespace progressive {

pmr_vector<std::shared_ptr<AbstractSegment>> get_chunk_segments(const std::shared_ptr<const Chunk>& chunk) {
  const auto column_count = chunk->column_count();
  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
  segments.reserve(column_count);
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    segments.push_back(chunk->get_segment(column_id));
  }
  return segments;
}

std::shared_ptr<Chunk> recreate_non_const_chunk(const std::shared_ptr<const Chunk>& chunk) {
  return std::make_shared<Chunk>(get_chunk_segments(chunk), chunk->mvcc_data());
}

}  // namespace progressive

}  // namespace hyrise