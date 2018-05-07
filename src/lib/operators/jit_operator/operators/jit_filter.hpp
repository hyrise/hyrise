#pragma once

#include "abstract_jittable.hpp"

namespace opossum {

/* The JitFilter operator filters on a single boolean value and only passes on
 * tuple, for which that value is non-null and true.
 */
class JitFilter : public AbstractJittable {
 public:
  explicit JitFilter(const JitTupleValue& condition);

  std::string description() const final;

  JitTupleValue condition();

 private:
  void _consume(JitRuntimeContext& context) const final;

  const JitTupleValue _condition;
};

}  // namespace opossum
