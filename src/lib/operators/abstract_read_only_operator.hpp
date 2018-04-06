#pragma once

#include <memory>

#include "operators/abstract_operator.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * AbstractReadOnlyOperator is the superclass for all operators that not need write access to their input tables.
 */
class AbstractReadOnlyOperator : public AbstractOperator {
 public:
  using AbstractOperator::AbstractOperator;

 protected:
  // This override exists so that all AbstractReadOnlyOperators can ignore the transaction context
  // Apart from Validate, none of the read-only operators needs the transaction context.
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> /*context*/) override;

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
