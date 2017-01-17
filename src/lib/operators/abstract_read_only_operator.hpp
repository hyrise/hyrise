#pragma once

#include <memory>

#include "common.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

class AbstractReadOnlyOperator : public AbstractOperator {
 public:
  AbstractReadOnlyOperator(const std::shared_ptr<const AbstractOperator> left = nullptr,
                           const std::shared_ptr<const AbstractOperator> right = nullptr)
      : AbstractOperator(left, right) {}

  std::shared_ptr<const Table> on_execute(const TransactionContext* /* context */) override { return on_execute(); }

  virtual std::shared_ptr<const Table> on_execute() = 0;

 protected:
  // Some operators need an internal implementation class, mostly in cases where
  // their execute method depends on a template parameter. An example for this is
  // found in table_scan.hpp.
  class AbstractNonModifyingOperatorImpl {
   public:
    virtual ~AbstractNonModifyingOperatorImpl() = default;
    virtual std::shared_ptr<const Table> on_execute() = 0;
  };
};

}  // namespace opossum
