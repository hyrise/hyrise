#include "jit_operator_wrapper.hpp"

#include "expression/expression_utils.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"

namespace opossum {

JitOperatorWrapper::JitOperatorWrapper(const std::shared_ptr<const AbstractOperator>& left,
                                       const JitExecutionMode execution_mode,
                                       const std::shared_ptr<SpecializedFunctionWrapper>& specialized_function_wrapper)
    : AbstractReadOnlyOperator{OperatorType::JitOperatorWrapper, left},
      _execution_mode{execution_mode},
      _specialized_function_wrapper{specialized_function_wrapper} {}

const std::string JitOperatorWrapper::name() const { return "JitOperatorWrapper"; }

const std::string JitOperatorWrapper::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperatorWrapper]" << separator;
  for (const auto& op : _specialized_function_wrapper->jit_operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

void JitOperatorWrapper::add_jit_operator(const std::shared_ptr<AbstractJittable>& op) {
  _specialized_function_wrapper->jit_operators.push_back(op);
}

const std::vector<std::shared_ptr<AbstractJittable>>& JitOperatorWrapper::jit_operators() const {
  return _specialized_function_wrapper->jit_operators;
}

const std::vector<AllTypeVariant>& JitOperatorWrapper::input_parameter_values() const {
  return _input_parameter_values;
}

const std::shared_ptr<JitReadTuples> JitOperatorWrapper::_source() const {
  return std::dynamic_pointer_cast<JitReadTuples>(_specialized_function_wrapper->jit_operators.front());
}

const std::shared_ptr<AbstractJittableSink> JitOperatorWrapper::_sink() const {
  return std::dynamic_pointer_cast<AbstractJittableSink>(_specialized_function_wrapper->jit_operators.back());
}

std::shared_ptr<const Table> JitOperatorWrapper::_on_execute() {
  Assert(_source(), "JitOperatorWrapper does not have a valid source node.");
  Assert(_sink(), "JitOperatorWrapper does not have a valid sink node.");

  _prepare_and_specialize_operator_pipeline();

  const auto in_table = input_left()->get_output();

  auto out_table = _sink()->create_output_table(*in_table);

  JitRuntimeContext context;
  if (transaction_context_is_set()) {
    context.transaction_id = transaction_context()->transaction_id();
    context.snapshot_commit_id = transaction_context()->snapshot_commit_id();
  }

  _source()->before_query(*in_table, _input_parameter_values, context);
  _sink()->before_query(*out_table, context);

  const auto chunk_count = in_table->chunk_count();
  for (ChunkID chunk_id{0}; chunk_id < chunk_count && context.limit_rows; ++chunk_id) {
    const auto chunk = in_table->get_chunk(chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    bool use_specialized_function = _source()->before_chunk(*in_table, chunk_id, _input_parameter_values, context);
    if (use_specialized_function) {
      _specialized_function_wrapper->execute_func(_source().get(), context);
    } else {
      _source()->execute(context);
    }
    _sink()->after_chunk(in_table, *out_table, context);
  }

  _sink()->after_query(*out_table, context);

  return out_table;
}

void JitOperatorWrapper::_prepare_and_specialize_operator_pipeline() {
  // Use a mutex to specialize a jittable operator pipeline within a subquery only once.
  // See jit_operator_wrapper.hpp for details.
  std::lock_guard<std::mutex> guard(_specialized_function_wrapper->specialization_mutex);
  if (_specialized_function_wrapper->execute_func) return;

  const auto in_table = input_left()->get_output();

  const auto jit_operators = _specialized_function_wrapper->jit_operators;

  std::vector<bool> tuple_non_nullable_information;
  for (auto& jit_operator : jit_operators) {
    jit_operator->before_specialization(*in_table, tuple_non_nullable_information);
  }

  // Connect operators to a chain
  for (auto it = jit_operators.begin(); it != jit_operators.end() && it + 1 != jit_operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }

  std::function<void(const JitReadTuples*, JitRuntimeContext&)> execute_func;
  // We want to perform two specialization passes if the operator chain contains a JitAggregate operator, since the
  // JitAggregate operator contains multiple loops that need unrolling.
  auto two_specialization_passes = static_cast<bool>(std::dynamic_pointer_cast<JitAggregate>(_sink()));
  switch (_execution_mode) {
    case JitExecutionMode::Compile:
      // this corresponds to "opossum::JitReadTuples::execute(opossum::JitRuntimeContext&) const"
      _specialized_function_wrapper->execute_func =
          _specialized_function_wrapper->module
              .specialize_and_compile_function<void(const JitReadTuples*, JitRuntimeContext&)>(
                  "_ZNK7opossum13JitReadTuples7executeERNS_17JitRuntimeContextE",
                  std::make_shared<JitConstantRuntimePointer>(_source().get()), two_specialization_passes);
      break;
    case JitExecutionMode::Interpret:
      _specialized_function_wrapper->execute_func = &JitReadTuples::execute;
      break;
  }
}

void JitOperatorWrapper::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  const auto& input_parameters = _source()->input_parameters();
  _input_parameter_values.resize(input_parameters.size());

  for (size_t index{0}; index < input_parameters.size(); ++index) {
    const auto search = parameters.find(input_parameters[index].parameter_id);
    if (search != parameters.end()) {
      _input_parameter_values[index] = search->second;
    }
  }

  // Set any parameter values used within in the row count expression.
  if (const auto row_count_expression = _source()->row_count_expression) {
    expression_set_parameters(row_count_expression, parameters);
  }
}

void JitOperatorWrapper::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  // Set the MVCC data in the row count expression required by possible subqueries within the expression.
  if (const auto row_count_expression = _source()->row_count_expression) {
    expression_set_transaction_context(row_count_expression, transaction_context);
  }
}

std::shared_ptr<AbstractOperator> JitOperatorWrapper::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JitOperatorWrapper>(copied_input_left, _execution_mode, _specialized_function_wrapper);
}

}  // namespace opossum
