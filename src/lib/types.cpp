#include "types.hpp"

#include <string>

namespace opossum {

std::string to_string(const AllTypeVariant& x) { return boost::lexical_cast<std::string>(x); }

ChunkID get_chunk_id_from_row_id(RowID r_id) { return r_id >> MAX_CHUNK_SIZE; }

ChunkOffset get_chunk_offset_from_row_id(RowID r_id) { return r_id; }

RowID get_row_id_from_chunk_id_and_chunk_offset(ChunkID c_id, ChunkOffset c_offset) {
  return (static_cast<RowID>(c_id) << MAX_CHUNK_SIZE) + c_offset;
}
}  // namespace opossum
