#pragma once

#include "abstract_jittable_sink.hpp"

namespace opossum {

/* Base class for all column writers.
 * We need this class, so we can store a number of JitColumnWriters with different template
 * specializations in a common data structure.
 */
class BaseJitColumnWriter {
 public:
  virtual ~BaseJitColumnWriter() = default;
  virtual void write_value(JitRuntimeContext& context) const = 0;
};

struct JitOutputColumn {
  std::string column_name;
  JitTupleValue tuple_value;
};

/* JitWriteTuples must be the last operator in any chain of jit operators.
 * It is responsible for
 * 1) adding column definitions to the output table
 * 2) appending the current tuple to the current output chunk
 * 3) creating a new output chunks and adding output chunks to the output table
 */
class JitWriteTuples : public AbstractJittableSink {
  /* JitColumnWriters provide a template-free interface to store tuple values in ValueColumns in the output table.
   *
   * All ValueColumns have BaseValueColumn as their template-free super class. This allows us to store shared pointers
   * to all output columns in vector in the runtime context.
   * We then use JitColumnWriter instances to access these columns. JitColumnWriters are templated with the
   * type of ValueColumn they are accessing. They are initialized with an output_index and a tuple value.
   * When requested to store a value, they will access the column from the runtime context corresponding to their
   * output_index and copy the value from their JitTupleValue.
   *
   * All column writers have a common template-free base class. That allows us to store the column writers in a
   * vector as well and access all types of columns with a single interface.
   */
  template <typename ValueColumn, typename DataType, bool Nullable>
  class JitColumnWriter : public BaseJitColumnWriter {
   public:
    JitColumnWriter(const std::shared_ptr<ValueColumn>& column, const JitTupleValue& tuple_value)
        : _column{column}, _tuple_value{tuple_value} {}

    // Reads the value from the _tuple_value and appends it to the output ValueColumn.
    void write_value(JitRuntimeContext& context) const {
      _column->values().push_back(context.tuple.get<DataType>(_tuple_value.tuple_index()));
      // clang-format off
      if constexpr (Nullable) {
        _column->null_values().push_back(context.tuple.is_null(_tuple_value.tuple_index()));
      }
      // clang-format on
    }

   private:
    std::shared_ptr<ValueColumn> _column;
    const JitTupleValue _tuple_value;
  };

 public:
  std::string description() const final;

  std::shared_ptr<Table> create_output_table(const ChunkOffset input_table_chunk_size) const final;
  void before_query(Table& out_table, JitRuntimeContext& context) const override;
  void after_chunk(Table& out_table, JitRuntimeContext& context) const override;

  void add_output_column(const std::string& column_name, const JitTupleValue& value);

  std::vector<JitOutputColumn> output_columns() const;

 private:
  void _consume(JitRuntimeContext& context) const final;

  void _create_output_chunk(JitRuntimeContext& context) const;

  std::vector<JitOutputColumn> _output_columns;
};

}  // namespace opossum
