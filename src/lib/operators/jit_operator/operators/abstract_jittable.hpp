#pragma once

#include <vector>

#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

/**
 * AbstractJittable is the abstract super class of all operators used within a JitOperatorWrapper.
 * Usually, multiple operators are linked together to form an operator chain.
 * The operators work in a push-based fashion: The virtual "next" function is called for each tuple.
 * The operator can then process the tuple and finally call its own "emit" function to pass the tuple
 * on to the next operator in the chain.
 */
class AbstractJittable {
 public:
  virtual ~AbstractJittable() = default;

  void set_next_operator(const std::shared_ptr<AbstractJittable>& next_operator) { _next_operator = next_operator; }

  std::shared_ptr<AbstractJittable> next_operator() { return _next_operator; }

  virtual std::string description() const = 0;

 protected:
  void _emit(JitRuntimeContext& context) const { _next_operator->_consume(context); }

 private:
  virtual void _consume(JitRuntimeContext& context) const = 0;

  std::shared_ptr<AbstractJittable> _next_operator;
};

}  // namespace opossum
