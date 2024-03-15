#pragma once

#include <memory>

#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace hyrise {

namespace progressive {

pmr_vector<std::shared_ptr<AbstractSegment>> get_chunk_segments(const std::shared_ptr<const Chunk>& chunk);

std::shared_ptr<Chunk> recreate_non_const_chunk(const std::shared_ptr<const Chunk>& chunk);

}  // namespace progressive

}  // namespace hyrise