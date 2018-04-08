#include "jit_operator_wrapper.hpp"

namespace opossum {

JitOperatorWrapper::JitOperatorWrapper(const std::shared_ptr<const AbstractOperator> left, const bool use_jit,
                                       const std::vector<std::shared_ptr<AbstractJittable>>& operators)
    : AbstractReadOnlyOperator{OperatorType::JitOperatorWrapper, left}, _use_jit{use_jit}, _operators{operators} {}

const std::string JitOperatorWrapper::name() const { return "JitOperatorWrapper"; }

const std::string JitOperatorWrapper::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperatorWrapper]" << separator;
  for (const auto& op : _operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

std::shared_ptr<AbstractOperator> JitOperatorWrapper::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<JitOperatorWrapper>(input_left()->recreate(args), _use_jit, _operators);
}

void JitOperatorWrapper::add_jit_operator(const std::shared_ptr<AbstractJittable>& op) { _operators.push_back(op); }

const std::shared_ptr<JitReadTuple> JitOperatorWrapper::_source() const {
  return std::dynamic_pointer_cast<JitReadTuple>(_operators.front());
}

const std::shared_ptr<AbstractJittableSink> JitOperatorWrapper::_sink() const {
  return std::dynamic_pointer_cast<AbstractJittableSink>(_operators.back());
}

std::shared_ptr<const Table> JitOperatorWrapper::_on_execute() {
  DebugAssert(_source(), "JitOperatorWrapper does not have a valid source node.");
  DebugAssert(_sink(), "JitOperatorWrapper does not have a valid sink node.");

  const auto& in_table = *input_left()->get_output();

  auto out_table = _sink()->create_output_table(in_table.max_chunk_size());

  JitRuntimeContext context;
  _source()->before_query(in_table, context);
  _sink()->before_query(*out_table, context);

  // Connect operators to a chain
  for (auto it = _operators.begin(); it != _operators.end() && it + 1 != _operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }

  std::function<void(const JitReadTuple*, JitRuntimeContext&)> execute_func;
  if (_use_jit) {
    execute_func = _module.specialize_function<void(const JitReadTuple*, JitRuntimeContext&)>(
        "_ZNK7opossum12JitReadTuple7executeERNS_17JitRuntimeContextE",
        std::make_shared<JitConstantRuntimePointer>(_source().get()), false);
  } else {
    execute_func = &JitReadTuple::execute;
  }

  for (opossum::ChunkID chunk_id{0}; chunk_id < in_table.chunk_count(); ++chunk_id) {
    const auto& in_chunk = *in_table.get_chunk(chunk_id);

    context.chunk_size = in_chunk.size();
    context.chunk_offset = 0;

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
  // What should happen here? What are the semantics of recreate?
  return nullptr;
}

// cleanup
#undef JIT_ABSTRACT_SOURCE_EXECUTE_MANGLED_NAME

}  // namespace opossum
