#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

#include "storage/encoding_type.hpp"

namespace opossum {

class BaseColumn;
class Chunk;
class Table;

/**
 * @brief Compresses parts of or an entire table using one encoding type
 *
 * @deprecated Use ChunkEncoder interface instead!
 */
class DeprecatedDictionaryCompression {
 public:
  /**
   * @brief Compresses a column
   *
   * This is potentially unsafe if another operation modifies the table at the same time. In most cases, this should
   * only be called by the ChunkCompressionTask.
   *
   * @param data_type enum value of the column’s type
   * @param column needs to be of type ValueColumn<T>
   * @return a compressed column of type BaseEncodedColumn
   */
  static std::shared_ptr<BaseColumn> compress_column(DataType data_type, const std::shared_ptr<BaseColumn>& column,
                                                     EncodingType encoding_type = EncodingType::DeprecatedDictionary);

  /**
   * @brief Compresses a chunk
   *
   * Compresses the passed chunk by compressing each column
   * and reducing the fragmentation of its mvcc columns
   * All columns of the chunk need to be of type ValueColumn<T>
   *
   * This is potentially unsafe if another operation modifies the table at the same time. In most cases, this should
   * only be called by the ChunkCompressionTask.
   *
   * @param column_types from the chunk’s table
   * @param chunk to be compressed
   */
  static void compress_chunk(const std::vector<DataType>& column_types, const std::shared_ptr<Chunk>& chunk,
                             EncodingType encoding_type = EncodingType::DeprecatedDictionary);

  /**
   * @brief Compresses specified chunks of a table
   *
   * This is potentially unsafe if another operation modifies the table at the same time. In most cases, this should
   * only be called by the ChunkCompressionTask.
   */
  static void compress_chunks(Table& table, const std::vector<ChunkID>& chunk_ids,
                              EncodingType encoding_type = EncodingType::DeprecatedDictionary);

  /**
   * @brief Compresses a table by calling compress_chunk for each chunk
   *
   * This is potentially unsafe if another operation modifies the table at the same time. In most cases, this should
   * only be called by the ChunkCompressionTask.
   */
  static void compress_table(Table& table, EncodingType encoding_type = EncodingType::DeprecatedDictionary);
};

}  // namespace opossum
