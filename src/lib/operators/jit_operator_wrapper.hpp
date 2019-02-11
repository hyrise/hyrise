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
  /* The SpecializedFunctionWrapper allows the JitOperatorWrapper to share a jittable operator pipeline and the
   * specialized function from this pipeline between multiple JitOperatorWrapper instances. The mutex ensures that the
   * same pipeline within a correlated subquery is specialized only once when executed in parallel.
   *
   * During the evaluation of a correlated subquery, multiple subqueries can be executed in parallel. If no mutex is
   * used, the first executed JitOperatorWrapper instance will start specializing the jittable operator pipeline which
   * takes a considerate amount of time. If a second JitOperatorWrapper instance is executed in parallel, the second
   * instance will also start specializing the pipeline as no specialized function exists so far.
   * To prevent this, a mutex is used during specialization which ensures that only the first JitOperatorWrapper
   * instance specializes the pipeline and all other instances wait till the specialization finishes.
   */
  struct SpecializedFunctionWrapper {
    std::vector<std::shared_ptr<AbstractJittable>> jit_operators;
    std::function<void(const JitReadTuples*, JitRuntimeContext&)> execute_func;
    std::mutex specialization_mutex;
    JitCodeSpecializer module;
  };

  explicit JitOperatorWrapper(const std::shared_ptr<const AbstractOperator>& left,
                              const JitExecutionMode execution_mode = JitExecutionMode::Compile,
                              const std::shared_ptr<SpecializedFunctionWrapper>& specialized_function_wrapper =
                                  std::make_shared<SpecializedFunctionWrapper>());

  const std::string name() const final;
  const std::string description(DescriptionMode description_mode) const final;

  // Adds a jittable operator to the end of the operator pipeline.
  // The operators will later be chained by the JitOperatorWrapper.
  void add_jit_operator(const std::shared_ptr<AbstractJittable>& op);

  const std::vector<std::shared_ptr<AbstractJittable>>& jit_operators() const;
  const std::vector<AllTypeVariant>& input_parameter_values() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;

 private:
  const std::shared_ptr<JitReadTuples> _source() const;
  const std::shared_ptr<AbstractJittableSink> _sink() const;

  void _prepare_and_specialize_operator_pipeline();

  const JitExecutionMode _execution_mode;
  const std::shared_ptr<SpecializedFunctionWrapper> _specialized_function_wrapper;

  std::vector<AllTypeVariant> _input_parameter_values;
};

}  // namespace opossum
