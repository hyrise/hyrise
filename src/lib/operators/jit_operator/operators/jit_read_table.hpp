#pragma once

#include "jit_abstract_operator.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

class BaseJitColumnReader {
 public:
  using Ptr = std::shared_ptr<const BaseJitColumnReader>;

  virtual void read_value(JitRuntimeContext& ctx) const = 0;
  virtual void increment(JitRuntimeContext& ctx) const = 0;
};

template <typename Derived, typename DataType, bool Nullable>
class JitColumnReader : public BaseJitColumnReader {
 public:
  JitColumnReader(const size_t input_index, const JitTupleValue& tuple_value)
      : _input_index{input_index}, _tuple_value{tuple_value} {}

  void read_value(JitRuntimeContext& ctx) const {
    const auto& value = *_iterator(ctx);
    // clang-format off
    if constexpr (Nullable) {
      _tuple_value.materialize(ctx).is_null() = value.is_null();
      if (!value.is_null()) {
        _tuple_value.materialize(ctx).template as<DataType>() = value.value();
      }
    } else {
      _tuple_value.materialize(ctx).template as<DataType>() = value.value();
    }
    // clang-format on
  }

  void increment(JitRuntimeContext& ctx) const final { ++_iterator(ctx); }

 private:
  Derived& _iterator(JitRuntimeContext& ctx) const {
    return *std::static_pointer_cast<Derived>(ctx.inputs[_input_index]);
  }

  const size_t _input_index;
  const JitTupleValue _tuple_value;
};

class JitReadTable : public JitAbstractOperator {
 public:
  using Ptr = std::shared_ptr<JitReadTable>;

  std::string description() const final;

  void before_query(const Table& in_table, JitRuntimeContext& ctx);
  void before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& ctx) const;

  JitTupleValue add_input_column(const Table& table, const ColumnID column_id);
  JitTupleValue add_literal_value(const AllTypeVariant& value);
  size_t add_temorary_value();

  void execute(JitRuntimeContext& ctx) const;

 protected:
  uint32_t _num_tuple_values{0};
  std::vector<std::pair<ColumnID, JitTupleValue>> _input_columns;
  std::vector<std::pair<AllTypeVariant, JitTupleValue>> _input_literals;
  std::vector<BaseJitColumnReader::Ptr> _column_readers;

 private:
  void next(JitRuntimeContext& ctx) const final {}
};

}  // namespace opossum
