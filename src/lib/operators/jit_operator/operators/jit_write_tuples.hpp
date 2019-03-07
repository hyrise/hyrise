#pragma once

#include "abstract_jittable_sink.hpp"

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
  JitTupleEntry tuple_entry;
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
   * type of ValueSegment they are accessing. They are initialized with an output_index and a tuple entry.
   * When requested to store a value, they will access the column from the runtime context corresponding to their
   * output_index and copy the value from their JitTupleEntry.
   *
   * All segment writers have a common template-free base class. That allows us to store the segment writers in a
   * vector as well and access all types of segments with a single interface.
   */
  template <typename ValueSegment, typename DataType, bool Nullable>
  class JitSegmentWriter : public BaseJitSegmentWriter {
   public:
    JitSegmentWriter(const std::shared_ptr<ValueSegment>& segment, const JitTupleEntry& tuple_entry)
        : _segment{segment}, _tuple_entry{tuple_entry} {}

    // Reads the value from the _tuple_entry and appends it to the output ValueSegment.
    void write_value(JitRuntimeContext& context) const {
      _segment->values().push_back(context.tuple.get<DataType>(_tuple_entry.tuple_index()));
      // clang-format off
      if constexpr (Nullable) {
        _segment->null_values().push_back(context.tuple.is_null(_tuple_entry.tuple_index()));
      }
      // clang-format on
    }

   private:
    std::shared_ptr<ValueSegment> _segment;
    const JitTupleEntry _tuple_entry;
  };

 public:
  std::string description() const final;

  // Is called by the JitOperatorWrapper.
  // Creates an empty output table with appropriate column definitions.
  std::shared_ptr<Table> create_output_table(const Table& in_table) const final;

  // Is called by the JitOperatorWrapper before any tuple is consumed.
  // This is used to initialize the JitSegmentWriters for the first chunk.
  void before_query(Table& out_table, JitRuntimeContext& context) const override;

  // Is called by the JitOperatorWrapper after all tuples of one chunk have been consumed.
  // This is used to append the created chunk to the output table and prepare the JitSegmentWriters for the next chunk.
  void after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                   JitRuntimeContext& context) const override;

  // Is called by the jit-aware LQP translator.
  // This is used to define which columns are in the output table.
  // The order in which the columns are added defines the order of the columns in the output table.
  void add_output_column_definition(const std::string& column_name, const JitTupleEntry& tuple_entry);

  std::vector<JitOutputColumn> output_columns() const;

 private:
  // Add tuple values to the corresponding value segments in the output chunk.
  void _consume(JitRuntimeContext& context) const final;

  void _create_output_chunk(JitRuntimeContext& context) const;

  std::vector<JitOutputColumn> _output_columns;
};

}  // namespace opossum
