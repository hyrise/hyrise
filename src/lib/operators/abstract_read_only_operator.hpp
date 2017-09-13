#pragma once

#include <memory>

#include "common.hpp"
#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * AbstractReadOnlyOperator is the superclass for all operators that not need write access to their input tables.
 */
class AbstractReadOnlyOperator : public AbstractOperator {
 public:
  AbstractReadOnlyOperator(const std::shared_ptr<const AbstractOperator> left = nullptr,
                           const std::shared_ptr<const AbstractOperator> right = nullptr)
      : AbstractOperator(left, right) {}

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> /*context*/) override {
    return _on_execute();
  }

  virtual std::shared_ptr<const Table> _on_execute() = 0;

  // Some operators need an internal implementation class, mostly in cases where
  // their execute method depends on a template parameter. An example for this is
  // found in table_scan.hpp.
  class AbstractReadOnlyOperatorImpl {
   public:
    virtual ~AbstractReadOnlyOperatorImpl() = default;
    virtual std::shared_ptr<const Table> _on_execute() = 0;
  };
};

}  // namespace opossum
