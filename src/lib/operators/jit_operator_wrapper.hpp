#pragma once

#include <string>

#include "abstract_read_only_operator.hpp"
#include "jit_operator/operators/abstract_jittable_sink.hpp"
#include "jit_operator/operators/jit_read_tuple.hpp"
#include "jit_operator/specialization/jit_module.hpp"

namespace opossum {

/* The JitOperatorWrapper wraps a number of jittable operators and exposes them through Hyrise's default
 * operator interface. This allows a number of jit operators to be seamlessly integrated with
 * the existing operator pipeline.
 * The JitOperatorWrapper is responsible for chaining the operators it contains, compiling code for the operators at
 * runtime, creating and managing the runtime context and calling hooks (before/after processing a chunk or the entire
 * query) on the its operators.
 */
class JitOperatorWrapper : public AbstractReadOnlyOperator {
 public:
  explicit JitOperatorWrapper(const std::shared_ptr<const AbstractOperator> left, const bool use_jit = true,
                              const std::vector<std::shared_ptr<AbstractJittable>>& operators = {});

  const std::string name() const final;
  const std::string description(DescriptionMode description_mode) const final;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const final;

  void compile_query();

  void add_jit_operator(const std::shared_ptr<AbstractJittable>& op);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

 private:
  const std::shared_ptr<JitReadTuple> _source() const;
  const std::shared_ptr<AbstractJittableSink> _sink() const;

  const bool _use_jit;
  std::vector<std::shared_ptr<AbstractJittable>> _operators;
  JitModule _module;
};

}  // namespace opossum
