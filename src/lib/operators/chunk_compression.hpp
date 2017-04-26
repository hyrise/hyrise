#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"

namespace opossum {

class BaseAttributeVector;

/**
 * @brief Compresses a chunk of a table
 *
 * The operators compresses a chunk by sequentially compressing columns.
 * From each value column, a dictionary column is created that replaces the
 * uncompressed column. The exchange is done atomically. Since this can
 * happen during simultaneous acces by transaction, operators need to be
 * design such that they are aware that column types might change from
 * ValueColumn<T> to DictionaryColumn<T> during execution.
 *
 * Exchanging columns does not interfere with the Delete operator because
 * it does not touch the columns. However, inserting records while simultaneously
 * compressing the chunk leads to inconsistent state. Therefore only chunks where
 * all insertion has been completed may be compressed. In other words, they need to be
 * full and all of their end-cids must be smaller than infinity. The operator calls
 * those chunks “completed”.
 *
 * After the columns have been replaced, the operator call Chunk::shrink_mvcc_columns()
 * in order to reduce fragmentation of the MVCC columns. The MVCC columns are locked
 * exclusively during this step.
 *
 * Note: Reference columns are not invalidated by this operator because the order in which
 *       records are stored does not change.
 */
class ChunkCompression : public AbstractOperator {
 public:
  explicit ChunkCompression(const std::string& table_name, const ChunkID chunk_id, bool check_completion = true);
  explicit ChunkCompression(const std::string& table_name, const std::vector<ChunkID>& chunk_ids,
                            bool check_completion = true);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  /**
   * @brief Compresses a column
   *
   * @param column_type string representation of its type
   * @param column needs to be of type ValueColumn<T>
   * @return a compressed column of type DictionaryColumn<T>
   */
  static std::shared_ptr<BaseColumn> compress_column(const std::string& column_type,
                                                     const std::shared_ptr<BaseColumn>& column);

 protected:
  std::shared_ptr<const Table> on_execute(std::shared_ptr<TransactionContext> context) override;

 private:
  /**
   * @brief Checks if a chunks is completed
   *
   * See class comment for further explanation
   */
  bool chunk_is_completed(const Chunk& chunk, const uint32_t max_chunk_size);

 private:
  const bool _check_completion;  ///< decides whether chunk_is_completed is called before compression
  const std::string _table_name;
  const std::vector<ChunkID> _chunk_ids;
};
}  // namespace opossum
