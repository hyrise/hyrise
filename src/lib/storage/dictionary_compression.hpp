#pragma once

#include <memory>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

class BaseColumn;
class Chunk;
class Table;

class DictionaryCompression {
 public:
  /**
   * @brief Compresses a column
   *
   * @param column_type string representation of its type
   * @param column needs to be of type ValueColumn<T>
   * @return a compressed column of type DictionaryColumn<T>
   */
  static std::shared_ptr<BaseColumn> compress_column(const std::string& column_type,
                                                     const std::shared_ptr<BaseColumn>& column);

  /**
   * @brief Compresses a chunk
   *
   * Compresses the passed chunk by compressing each column
   * and reducing the fragmentation of its mvcc columns
   * All columns of the chunk need to be of type ValueColumn<T>
   *
   * @param column_types from the chunk’s table
   * @param chunk to be compressed
   */
  static void compress_chunk(const alloc_vector<std::string>& column_types, Chunk& chunk);

  /**
   * @brief Compresses specified chunks of a table
   */
  static void compress_chunks(Table& table, const alloc_vector<ChunkID>& chunk_ids);

  /**
   * @brief Compresses a table by calling compress_chunk for each chunk
   */
  static void compress_table(Table& table);
};

}  // namespace opossum
