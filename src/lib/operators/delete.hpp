#pragma once

#include <memory>
#include <string>

#include "abstract_read_write_operator.hpp"
#include "table_scan.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class Delete : public AbstractReadWriteOperator {
 public:
  explicit Delete(const std::shared_ptr<const AbstractOperator>& op);

  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override;

  void commit(const uint32_t cid) override;
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 private:
  std::shared_ptr<const PosList> _pos_list;
  std::shared_ptr<Table> _referenced_table;
  uint32_t _tid;
};
}  // namespace opossum
