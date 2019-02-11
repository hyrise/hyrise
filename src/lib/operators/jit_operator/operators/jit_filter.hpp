#pragma once

#include "abstract_jittable.hpp"

namespace opossum {

class JitExpression;

/* The JitFilter operator filters on a single boolean value and only passes on
 * tuple, for which that value is non-null and true.
 */
class JitFilter : public AbstractJittable {
 public:
  explicit JitFilter(const std::shared_ptr<JitExpression>& expression);

  std::string description() const final;

  std::shared_ptr<JitExpression> expression();

 private:
  void _consume(JitRuntimeContext& context) const final;

  const std::shared_ptr<JitExpression> _expression;
};

}  // namespace opossum
