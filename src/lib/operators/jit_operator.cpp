#include "jit_operator.hpp"

namespace opossum {

JitOperator::JitOperator(const std::shared_ptr<const AbstractOperator> left,
                         const std::vector<std::shared_ptr<JitAbstractOperator>>& operators)
    : AbstractReadOnlyOperator{left}, _operators{operators} {}

const std::string JitOperator::name() const { return "JitOperator"; }

const std::string JitOperator::description(DescriptionMode description_mode) const {
  std::stringstream desc;
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  desc << "[JitOperator]" << separator;
  for (const auto& op : _operators) {
    desc << op->description() << separator;
  }
  return desc.str();
}

std::shared_ptr<AbstractOperator> JitOperator::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<JitOperator>(input_left()->recreate(args), _operators);
}

void JitOperator::add_jit_operator(const std::shared_ptr<JitAbstractOperator>& op) { _operators.push_back(op); }

const std::shared_ptr<JitReadTuple> JitOperator::_source() const {
  return std::dynamic_pointer_cast<JitReadTuple>(_operators.front());
}

const std::shared_ptr<JitAbstractSink> JitOperator::_sink() const {
  return std::dynamic_pointer_cast<JitAbstractSink>(_operators.back());
}

std::shared_ptr<const Table> JitOperator::_on_execute() {
  // Connect operators to a chain
  for (auto it = _operators.begin(); it != _operators.end() && it + 1 != _operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }

  DebugAssert(_source(), "JitOperator does not have a valid source node.");
  DebugAssert(_sink(), "JitOperator does not have a valid sink node.");

  const auto& in_table = *input_left()->get_output();
  auto out_table = std::make_shared<opossum::Table>(in_table.max_chunk_size());

  JitRuntimeContext context;
  _source()->before_query(in_table, context);
  _sink()->before_query(*out_table, context);

  for (opossum::ChunkID chunk_id{0}; chunk_id < in_table.chunk_count(); ++chunk_id) {
    const auto& in_chunk = *in_table.get_chunk(chunk_id);

    context.chunk_size = in_chunk.size();
    context.chunk_offset = 0;

    _source()->before_chunk(in_table, in_chunk, context);
    _source()->execute(context);
    _sink()->after_chunk(*out_table, context);
  }

  return out_table;
}

}  // namespace opossum
