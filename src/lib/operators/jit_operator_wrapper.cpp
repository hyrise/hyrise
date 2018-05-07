#include "jit_operator_wrapper.hpp"

namespace opossum {

JitOperatorWrapper::JitOperatorWrapper(const std::shared_ptr<const AbstractOperator> left,
                                       const JitExecutionMode execution_mode,
                                       const std::vector<std::shared_ptr<AbstractJittable>>& jit_operators)
    : AbstractReadOnlyOperator{OperatorType::JitOperatorWrapper, left},
      _execution_mode{execution_mode},
      _jit_operators{jit_operators} {}

const std::string JitOperatorWrapper::name() const { return "JitOperatorWrapper"; }

const std::string JitOperatorWrapper::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperatorWrapper]" << separator;
  for (const auto& op : _jit_operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

void JitOperatorWrapper::add_jit_operator(const std::shared_ptr<AbstractJittable>& op) { _jit_operators.push_back(op); }

std::vector<std::shared_ptr<AbstractJittable>>& JitOperatorWrapper::jit_operators() { return _jit_operators; }

const std::shared_ptr<JitReadTuples> JitOperatorWrapper::_source() const {
  return std::dynamic_pointer_cast<JitReadTuples>(_jit_operators.front());
}

const std::shared_ptr<AbstractJittableSink> JitOperatorWrapper::_sink() const {
  return std::dynamic_pointer_cast<AbstractJittableSink>(_jit_operators.back());
}

std::shared_ptr<const Table> JitOperatorWrapper::_on_execute() {
  Assert(_source(), "JitOperatorWrapper does not have a valid source node.");
  Assert(_sink(), "JitOperatorWrapper does not have a valid sink node.");

  const auto& in_table = *input_left()->get_output();

  auto out_table = _sink()->create_output_table(in_table.max_chunk_size());

  JitRuntimeContext context;
  _source()->before_query(in_table, context);
  _sink()->before_query(*out_table, context);

  // Connect operators to a chain
  for (auto it = _jit_operators.begin(); it != _jit_operators.end() && it + 1 != _jit_operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }

  std::function<void(const JitReadTuples*, JitRuntimeContext&)> execute_func;
  switch (_execution_mode) {
    case JitExecutionMode::Compile:
      execute_func = _module.specialize_function<void(const JitReadTuples*, JitRuntimeContext&)>(
          "_ZNK7opossum13JitReadTuples7executeERNS_17JitRuntimeContextE",
          std::make_shared<JitConstantRuntimePointer>(_source().get()), false);
      break;
    case JitExecutionMode::Interpret:
      execute_func = &JitReadTuples::execute;
      break;
  }

  for (opossum::ChunkID chunk_id{0}; chunk_id < in_table.chunk_count(); ++chunk_id) {
    const auto& in_chunk = *in_table.get_chunk(chunk_id);
    _source()->before_chunk(in_table, in_chunk, context);
    execute_func(_source().get(), context);
    _sink()->after_chunk(*out_table, context);
  }

  _sink()->after_query(*out_table, context);

  return out_table;
}

std::shared_ptr<AbstractOperator> JitOperatorWrapper::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<JitOperatorWrapper>(recreated_input_left, _execution_mode, _jit_operators);
}

}  // namespace opossum
