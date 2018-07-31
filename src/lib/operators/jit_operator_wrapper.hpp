#pragma once

#include <string>

#include "abstract_read_only_operator.hpp"
#include "jit_operator/operators/abstract_jittable_sink.hpp"
#include "jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/specialization/jit_code_specializer.hpp"

namespace opossum {

enum class JitExecutionMode { Interpret, Compile };

/* The JitOperatorWrapper wraps a number of jittable operators and exposes them through Hyrise's default
 * operator interface. This allows a number of jit operators to be seamlessly integrated with
 * the existing operator pipeline.
 * The JitOperatorWrapper is responsible for chaining the operators it contains, compiling code for the operators at
 * runtime, creating and managing the runtime context and calling hooks (before/after processing a chunk or the entire
 * query) on the its operators.
 */
class JitOperatorWrapper : public AbstractReadOnlyOperator {
 public:
  explicit JitOperatorWrapper(const std::shared_ptr<const AbstractOperator>& left,
                              const JitExecutionMode execution_mode = JitExecutionMode::Compile,
                              const std::vector<std::shared_ptr<AbstractJittable>>& jit_operators = {});

  const std::string name() const final;
  const std::string description(DescriptionMode description_mode) const final;

  // Adds a jittable operator to the end of the operator pipeline.
  // The operators will later be chained by the JitOperatorWrapper.
  void add_jit_operator(const std::shared_ptr<AbstractJittable>& op);

  const std::vector<std::shared_ptr<AbstractJittable>>& jit_operators() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  const std::shared_ptr<JitReadTuples> _source() const;
  const std::shared_ptr<AbstractJittableSink> _sink() const;

  const JitExecutionMode _execution_mode;
  JitCodeSpecializer _module;
  std::vector<std::shared_ptr<AbstractJittable>> _jit_operators;
};

}  // namespace opossum
