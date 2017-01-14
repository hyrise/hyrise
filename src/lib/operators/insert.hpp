#pragma once

#include <memory>
#include <string>

#include "abstract_modifying_operator.hpp"
#include "get_table.hpp"

namespace opossum {

class TransactionContext;

// operator to retrieve a table from the StorageManager by specifying its name
class Insert : public AbstractModifyingOperator {
 public:
  explicit Insert(std::shared_ptr<GetTable> get_table, std::vector<AllTypeVariant>&& values);

  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override;
  void commit(const uint32_t cid) override;
  void abort() override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;

 protected:
  std::vector<AllTypeVariant> _values;
};
}  // namespace opossum
