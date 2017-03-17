#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"

namespace opossum {

class BaseAttributeVector;

/**
 * @brief Compresses a chunk of a table
 */
class ChunkCompression : public AbstractOperator {
 public:
  explicit ChunkCompression(const std::string& table_name, const ChunkID chunk_id);
  explicit ChunkCompression(const std::string& table_name, const std::vector<ChunkID>& chunk_ids);

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
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;

 private:
  const std::string& _table_name;
  const std::vector<ChunkID> _chunk_ids;
};
}  // namespace opossum
