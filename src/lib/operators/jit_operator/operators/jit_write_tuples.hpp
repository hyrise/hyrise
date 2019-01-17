#pragma once

#include "abstract_jittable_sink.hpp"  // NEEDEDINCLUDE

namespace opossum {

/* Base class for all column writers.
 * We need this class, so we can store a number of JitSegmentWriters with different template
 * specializations in a common data structure.
 */
class BaseJitSegmentWriter {
 public:
  virtual ~BaseJitSegmentWriter() = default;
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
  /* JitSegmentWriters provide a template-free interface to store tuple values in ValueSegments in the output table.
   *
   * All ValueSegments have BaseValueSegment as their template-free super class. This allows us to store shared pointers
   * to all output segments in vector in the runtime context.
   * We then use JitSegmentWriter instances to access these segments. JitSegmentWriters are templated with the
   * type of ValueSegment they are accessing. They are initialized with an output_index and a tuple value.
   * When requested to store a value, they will access the column from the runtime context corresponding to their
   * output_index and copy the value from their JitTupleValue.
   *
   * All segment writers have a common template-free base class. That allows us to store the segment writers in a
   * vector as well and access all types of segments with a single interface.
   */
  template <typename ValueSegment, typename DataType, bool Nullable>
  class JitSegmentWriter : public BaseJitSegmentWriter {
   public:
    JitSegmentWriter(const std::shared_ptr<ValueSegment>& segment, const JitTupleValue& tuple_value)
        : _segment{segment}, _tuple_value{tuple_value} {}

    // Reads the value from the _tuple_value and appends it to the output ValueSegment.
    void write_value(JitRuntimeContext& context) const {
      _segment->values().push_back(context.tuple.get<DataType>(_tuple_value.tuple_index()));
      // clang-format off
      if constexpr (Nullable) {
        _segment->null_values().push_back(context.tuple.is_null(_tuple_value.tuple_index()));
      }
      // clang-format on
    }

   private:
    std::shared_ptr<ValueSegment> _segment;
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
