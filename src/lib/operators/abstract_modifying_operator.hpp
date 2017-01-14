#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class AbstractModifyingOperator : public AbstractOperator {
 public:
  explicit AbstractModifyingOperator(const std::shared_ptr<const AbstractOperator>& op)
      : AbstractOperator(op), _succeeded{true} {}

  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override = 0;
  virtual void commit(const uint32_t cid) = 0;
  virtual void abort() = 0;

  bool succeeded() const { return _succeeded; }

  uint8_t num_out_tables() const override { return 0; };

 protected:
  std::vector<RowID> _modified_rows;
  bool _succeeded;  // false if transaction needs to be aborted
};

}  // namespace opossum
