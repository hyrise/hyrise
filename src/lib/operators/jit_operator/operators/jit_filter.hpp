#pragma once

#include "jit_abstract_operator.hpp"

namespace opossum {

class JitFilter : public JitAbstractOperator {
 public:
  explicit JitFilter(const JitTupleValue& condition);

  std::string description() const final;

 private:
  void next(JitRuntimeContext& ctx) const final;

  const JitTupleValue _condition;
};

}  // namespace opossum
