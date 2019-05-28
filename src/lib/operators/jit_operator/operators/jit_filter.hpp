#pragma once

#include "abstract_jittable.hpp"

namespace opossum {

class JitExpression;

/* The JitFilter operator computes a JitExpression returning a boolean value and only passes on
 * tuple, for which that value is non-null and true.
 */
class JitFilter : public AbstractJittable {
 public:
  explicit JitFilter(const std::shared_ptr<JitExpression>& expression);

  void before_specialization(const Table& in_table, std::vector<bool>& tuple_non_nullable_information) override;

  std::string description() const final;

  const std::shared_ptr<JitExpression> expression;

 private:
  void _consume(JitRuntimeContext& context) const final;
};

}  // namespace opossum
