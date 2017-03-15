#pragma once

#include <memory>
#include <string>

#include "abstract_read_write_operator.hpp"

namespace opossum {

/**
 * @brief Compresses a chunk of a table
 */
class ChunkCompression : public AbstractReadWriteOperator {
 public:
  explicit ChunkCompression(const std::string& table_name, const ChunkID chunk_id);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute(TransactionContext* context) override;

 private:
  const std::string& _table_name;
  const ChunkID _chunk_id;
};
}  // namespace opossum
