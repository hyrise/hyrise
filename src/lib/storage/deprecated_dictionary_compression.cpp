#include "deprecated_dictionary_compression.hpp"

#include <memory>
#include <vector>

#include "base_value_column.hpp"
#include "chunk.hpp"
#include "table.hpp"
#include "types.hpp"

#include "storage/base_encoded_column.hpp"
#include "storage/column_encoding_utils.hpp"
#include "storage/encoding_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<BaseColumn> DeprecatedDictionaryCompression::compress_column(DataType data_type,
                                                                             const std::shared_ptr<BaseColumn>& column,
                                                                             EncodingType encoding_type) {
  auto value_column = std::dynamic_pointer_cast<BaseValueColumn>(column);
  DebugAssert(value_column != nullptr, "Column must be uncompressed, i.e. a ValueColumn.");

  return encode_column(encoding_type, data_type, value_column);
}

void DeprecatedDictionaryCompression::compress_chunk(const std::vector<DataType>& column_types,
                                                     const std::shared_ptr<Chunk>& chunk, EncodingType encoding_type) {
  DebugAssert((column_types.size() == chunk->column_count()),
              "Number of column types does not match the chunk’s column count.");

  for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
    auto value_column = chunk->get_mutable_column(column_id);
    auto dict_column = compress_column(column_types[column_id], value_column, encoding_type);
    chunk->replace_column(column_id, dict_column);
  }

  if (chunk->has_mvcc_columns()) {
    chunk->shrink_mvcc_columns();
  }
}

void DeprecatedDictionaryCompression::compress_chunks(Table& table, const std::vector<ChunkID>& chunk_ids,
                                                      EncodingType encoding_type) {
  for (auto chunk_id : chunk_ids) {
    Assert(chunk_id < table.chunk_count(), "Chunk with given ID does not exist.");

    compress_chunk(table.column_types(), table.get_chunk(chunk_id), encoding_type);
  }
}

void DeprecatedDictionaryCompression::compress_table(Table& table, EncodingType encoding_type) {
  for (ChunkID chunk_id{0}; chunk_id < table.chunk_count(); ++chunk_id) {
    auto chunk = table.get_chunk(chunk_id);
    compress_chunk(table.column_types(), chunk, encoding_type);
  }
}

}  // namespace opossum
