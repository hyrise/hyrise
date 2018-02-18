#pragma once

#include "jit_abstract_sink.hpp"

namespace opossum {

class BaseJitColumnWriter {
 public:
  using Ptr = std::shared_ptr<const BaseJitColumnWriter>;

  virtual void write_value(JitRuntimeContext& ctx) const = 0;
};

template <typename Derived, typename DataType, bool Nullable>
class JitColumnWriter : public BaseJitColumnWriter {
 public:
  JitColumnWriter(const size_t output_index, const JitTupleValue& tuple_value)
      : _output_index{output_index}, _tuple_value{tuple_value} {}

  void write_value(JitRuntimeContext& ctx) const {
    const auto value = _tuple_value.materialize(ctx).template as<DataType>();
    _value_column(ctx).values().push_back(value);
    // clang-format off
    if constexpr (Nullable) {
      const auto is_null = _tuple_value.materialize(ctx).is_null();
      _value_column(ctx).null_values().push_back(is_null);
    }
    // clang-format on
  }

 private:
  Derived& _value_column(JitRuntimeContext& ctx) const {
    return *std::static_pointer_cast<Derived>(ctx.outputs[_output_index]);
  }

  const size_t _output_index;
  const JitTupleValue _tuple_value;
};

class JitSaveTable : public JitAbstractSink {
 public:
  std::string description() const final;

  void before_query(Table& out_table, JitRuntimeContext& ctx) final;
  void after_chunk(Table& out_table, JitRuntimeContext& ctx) const final;

  void add_output_column(const std::string& column_name, const JitTupleValue& tuple_value);

 private:
  void next(JitRuntimeContext& ctx) const final;

  void _create_output_chunk(JitRuntimeContext& ctx) const;

  std::vector<std::pair<std::string, JitTupleValue>> _output_columns;
  std::vector<BaseJitColumnWriter::Ptr> _column_writers;
};

}  // namespace opossum
