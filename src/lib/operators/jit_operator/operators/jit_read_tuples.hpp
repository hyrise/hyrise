#pragma once

#include "abstract_jittable.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

/* Base class for all column readers.
 * We need this class, so we can store a number of JitColumnReaders with different template
 * specializations in a common data structure.
 */
class BaseJitColumnReader {
 public:
  virtual ~BaseJitColumnReader() = default;
  virtual void read_value(JitRuntimeContext& context) = 0;
};

struct JitInputColumn {
  ColumnID column_id;
  JitTupleValue tuple_value;
};

struct JitInputLiteral {
  AllTypeVariant value;
  JitTupleValue tuple_value;
};

/* JitReadTuples must be the first operator in any chain of jit operators.
 * It is responsible for:
 * 1) storing literal values to the runtime tuple before the query is executed
 * 2) reading data from the the input table to the runtime tuple
 * 3) advancing the column iterators
 * 4) keeping track of the number of values in the runtime tuple. Whenever
 *    another operator needs to store a temporary value in the runtime tuple,
 *    it can request a slot in the tuple from JitReadTuples.
 */
class JitReadTuples : public AbstractJittable {
  /* JitColumnReaders wrap the column iterable interface used by most operators and makes it accessible
   * to the JitOperatorWrapper.
   *
   * Why we need this wrapper:
   * Most operators access data by creating a fixed number (usually one or two) of column iterables and
   * then immediately use those iterators in a lambda. The JitOperatorWrapper, on the other hand, processes
   * data in a tuple-at-a-time fashion and thus needs access to an arbitrary number of column iterators
   * at the same time.
   *
   * We solve this problem by introducing a template-free super class to all column iterators. This allows us to
   * create an iterator for each input column (before processing each chunk) and store these iterators in a
   * common vector in the runtime context.
   * We then use JitColumnReader instances to access these iterators. JitColumnReaders are templated with the
   * type of iterator they are supposed to handle. They are initialized with an input_index and a tuple value.
   * When requested to read a value, they will access the iterator from the runtime context corresponding to their
   * input_index and copy the value to their JitTupleValue.
   *
   * All column readers have a common template-free base class. That allows us to store the column readers in a
   * vector as well and access all types of columns with a single interface.
   */
  template <typename Iterator, typename DataType, bool Nullable>
  class JitColumnReader : public BaseJitColumnReader {
   public:
    JitColumnReader(const Iterator& iterator, const JitTupleValue& tuple_value)
        : _iterator{iterator}, _tuple_value{tuple_value} {}

    // Reads a value from the _iterator into the _tuple_value and increments the _iterator.
    void read_value(JitRuntimeContext& context) {
      const auto& value = *_iterator;
      ++_iterator;
      // clang-format off
      if constexpr (Nullable) {
        context.tuple.set_is_null(_tuple_value.tuple_index(), value.is_null());
        if (!value.is_null()) {
          context.tuple.set<DataType>(_tuple_value.tuple_index(), value.value());
        }
      } else {
        context.tuple.set<DataType>(_tuple_value.tuple_index(), value.value());
      }
      // clang-format on
    }

   private:
    Iterator _iterator;
    JitTupleValue _tuple_value;
  };

 public:
  std::string description() const final;

  virtual void before_query(const Table& in_table, JitRuntimeContext& context) const;
  virtual void before_chunk(const Table& in_table, const Chunk& in_chunk, JitRuntimeContext& context) const;

  JitTupleValue add_input_column(const DataType data_type, const bool is_nullable, const ColumnID column_id);
  JitTupleValue add_literal_value(const AllTypeVariant& value);
  size_t add_temporary_value();

  std::vector<JitInputColumn> input_columns() const;
  std::vector<JitInputLiteral> input_literals() const;

  std::optional<ColumnID> find_input_column(const JitTupleValue& tuple_value) const;
  std::optional<AllTypeVariant> find_literal_value(const JitTupleValue& tuple_value) const;

  void execute(JitRuntimeContext& context) const;

 protected:
  uint32_t _num_tuple_values{0};
  std::vector<JitInputColumn> _input_columns;
  std::vector<JitInputLiteral> _input_literals;

 private:
  void _consume(JitRuntimeContext& context) const final {}
};

}  // namespace opossum
