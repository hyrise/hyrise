#pragma once

#include "abstract_jittable.hpp"

namespace opossum {

class JitExpression;

/* The JitCompute operator computes a single expression on the current tuple.
 * Most of the heavy lifting is done by the JitExpression itself.
 */
class JitCompute : public AbstractJittable {
 public:
  explicit JitCompute(const std::shared_ptr<JitExpression>& expression);

  void before_specialization(const Table& in_table, std::vector<bool>& tuple_non_nullable_information) override;

  std::string description() const final;

  const std::shared_ptr<JitExpression> expression;

 private:
  void _consume(JitRuntimeContext& context) const final;
};

}  // namespace opossum
