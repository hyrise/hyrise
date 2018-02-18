#include "jit_operator.hpp"

#include "jit_operator/specialization/module.hpp"

#ifdef __APPLE__
#define JIT_ABSTRACT_SOURCE_EXECUTE_MANGLED_NAME "_ZNK7opossum17JitReadTable7executeERNS_17JitRuntimeContextE"
#else
#define JIT_ABSTRACT_SOURCE_EXECUTE_MANGLED_NAME ""
#endif

namespace opossum {

JitOperator::JitOperator(const std::shared_ptr<const AbstractOperator> left, const bool use_jit)
    : AbstractReadOnlyOperator{left}, _use_jit{use_jit} {}

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

void JitOperator::add_jit_operator(const JitAbstractOperator::Ptr& op) { _operators.push_back(op); }

const JitReadTable::Ptr JitOperator::_source() const {
  return std::dynamic_pointer_cast<JitReadTable>(_operators.front());
}

const JitAbstractSink::Ptr JitOperator::_sink() const {
  return std::dynamic_pointer_cast<JitAbstractSink>(_operators.back());
}

std::shared_ptr<const Table> JitOperator::_on_execute() {
  // Connect operators to a chain
  for (auto it = _operators.begin(); it != _operators.end() && it + 1 != _operators.end(); ++it) {
    (*it)->set_next_operator(*(it + 1));
  }

  DebugAssert(_source(), "JitOperator does not have a valid source node.");
  DebugAssert(_sink(), "JitOperator does not have a valid source node.");

  const auto& in_table = *input_left()->get_output();
  auto out_table = std::make_shared<opossum::Table>(in_table.max_chunk_size());

  JitRuntimeContext ctx;
  _source()->before_query(in_table, ctx);
  _sink()->before_query(*out_table, ctx);

  opossum::Module module(JIT_ABSTRACT_SOURCE_EXECUTE_MANGLED_NAME);
  std::function<void(const JitReadTable*, JitRuntimeContext&)> execute_func;

  if (_use_jit) {
    auto start = std::chrono::high_resolution_clock::now();
    module.specialize(std::make_shared<ConstantRuntimePointer>(_source().get()));
    execute_func = module.compile<void(const JitReadTable*, JitRuntimeContext&)>();
    auto runtime = std::round(
        std::chrono::duration<double, std::micro>(std::chrono::high_resolution_clock::now() - start).count());
    std::cout << "jitting took " << runtime / 1000.0 << "ms" << std::endl;
  } else {
    execute_func = &JitReadTable::execute;
  }

  for (opossum::ChunkID chunk_id{0}; chunk_id < in_table.chunk_count(); ++chunk_id) {
    const auto& in_chunk = *in_table.get_chunk(chunk_id);

    ctx.chunk_size = in_chunk.size();
    ctx.chunk_offset = 0;

    _source()->before_chunk(in_table, in_chunk, ctx);
    execute_func(_source().get(), ctx);
    _sink()->after_chunk(*out_table, ctx);
  }

  return out_table;
}

}  // namespace opossum
