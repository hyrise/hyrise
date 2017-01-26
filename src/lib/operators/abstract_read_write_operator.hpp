#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class AbstractReadWriteOperator : public AbstractOperator {
 public:
  explicit AbstractReadWriteOperator(const std::shared_ptr<const AbstractOperator> left,
                                     const std::shared_ptr<const AbstractOperator> right = nullptr)
      : AbstractOperator(left, right), _execute_failed{false} {}

  std::shared_ptr<const Table> on_execute(const TransactionContext* context) override = 0;
  virtual void commit(const uint32_t cid) = 0;
  virtual void abort() = 0;

  bool execute_failed() const { return _execute_failed; }

  uint8_t num_out_tables() const override { return 0; };

 protected:
  bool _execute_failed;
};

}  // namespace opossum
