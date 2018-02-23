#pragma once

#include <string>

#include "abstract_read_only_operator.hpp"
#include "jit_operator/operators/jit_abstract_sink.hpp"
#include "jit_operator/operators/jit_read_tuple.hpp"

namespace opossum {

/* The JitOperator wraps a number of jitable operators and exposes them through Hyrise's default
 * operator interface. This allows a number of jit operators to be seamlessly integrated with
 * the existing operator pipeline.
 * The JitOperator is responsible for chaining the operators it contains, compiling code for the operators at runtime,
 * creating and managing the runtime context and calling hooks (before/after processing a chunk or the entire query)
 * on the its operators.
 */
class JitOperator : public AbstractReadOnlyOperator {
 public:
  explicit JitOperator(const std::shared_ptr<const AbstractOperator> left, const bool use_jit = true,
                       const std::vector<JitAbstractOperator::Ptr>& operators = {});

  const std::string name() const final;
  const std::string description(DescriptionMode description_mode) const final;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const final;

  void add_jit_operator(const JitAbstractOperator::Ptr& op);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  const JitReadTuple::Ptr _source() const;
  const JitAbstractSink::Ptr _sink() const;

  const bool _use_jit;
  std::vector<JitAbstractOperator::Ptr> _operators;
};

}  // namespace opossum
