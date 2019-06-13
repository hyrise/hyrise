#include "jit_aggregate.hpp"

#include "constant_mappings.hpp"
#include "operators/jit_operator/jit_operations.hpp"
#include "resolve_type.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

std::string JitAggregate::description() const {
  std::stringstream desc;
  desc << "[Aggregate] GroupBy: ";
  for (const auto& groupby_column : _groupby_columns) {
    desc << groupby_column.column_name << " = x" << groupby_column.tuple_entry.tuple_index << ", ";
  }
  desc << " Aggregates: ";
  for (const auto& aggregate_column : _aggregate_columns) {
    desc << aggregate_column.column_name << " = " << aggregate_column.function << "(x"
         << aggregate_column.tuple_entry.tuple_index << "), ";
  }
  return desc.str();
}

void JitAggregate::before_specialization(const Table& in_table, std::vector<bool>& tuple_non_nullable_information) {
  // Update the groupby and aggregate column nullable information from the tuple_non_nullable_information
  for (auto& groupby_column : _groupby_columns) {
    groupby_column.tuple_entry.guaranteed_non_null =
        tuple_non_nullable_information[groupby_column.tuple_entry.tuple_index];
    groupby_column.hashmap_entry.guaranteed_non_null =
        tuple_non_nullable_information[groupby_column.tuple_entry.tuple_index];
  }

  // Only update the input information (tuple_entry) to the aggregate function and not the output information
  // (hashmap_entry), which is already correct.
  for (auto& aggregate_column : _aggregate_columns) {
    aggregate_column.tuple_entry.guaranteed_non_null =
        tuple_non_nullable_information[aggregate_column.tuple_entry.tuple_index];
  }
}

std::shared_ptr<Table> JitAggregate::create_output_table(const Table& in_table) const {
  auto num_columns = _aggregate_columns.size() + _groupby_columns.size();
  TableColumnDefinitions column_definitions(num_columns);

  // Add each groupby column to the output table
  for (const auto& column : _groupby_columns) {
    const auto data_type = column.hashmap_entry.data_type;
    const auto is_nullable = !column.hashmap_entry.guaranteed_non_null;
    column_definitions[column.position_in_table] = {column.column_name, data_type, is_nullable};
  }

  // Add each aggregate to the output table
  for (const auto& column : _aggregate_columns) {
    // Computed averages are always of type double. The hashmap_entry, however, is used to compute a SUM aggregate
    // (that is later used to produce averages in a post-processing step) and may thus have a different data type.
    const auto data_type =
        column.function == AggregateFunction::Avg ? DataType::Double : column.hashmap_entry.data_type;
    const auto is_nullable = !column.hashmap_entry.guaranteed_non_null;
    column_definitions[column.position_in_table] = {column.column_name, data_type, is_nullable};
  }

  return std::make_shared<Table>(column_definitions, TableType::Data);
}

void JitAggregate::before_query(Table& out_table, JitRuntimeContext& context) const {
  // Resize the hashmap data structure.
  context.hashmap.columns.resize(_num_hashmap_columns);
}

// Performs the post-processing step for average aggregates.
// This operation is only possible for numeric data types, since averages on strings make no sense.
// However, the compiler does not understand that and requires an implementation for all data types.
// We thus rely on the SFINAE pattern to provide a fallback implementation that throws an exception.
// This is why this operation must be performed in a separate function.
template <typename ColumnDataType>
std::enable_if_t<std::is_arithmetic_v<ColumnDataType>, void> compute_averages(
    const std::vector<ColumnDataType>& sum_values, const std::vector<int64_t>& count_values,
    std::vector<double>& avg_values) {
  for (auto i = 0u; i < sum_values.size(); ++i) {
    // Avoid division by 0.
    // The COUNT of an average aggregate can only be 0, if only NULL values have been encountered as inputs to the
    // aggregate.
    // In this case the resulting average aggregate will also be NULL and there is no need to perform any division.
    if (count_values[i] > 0) {
      avg_values[i] = static_cast<double>(sum_values[i]) / static_cast<double>(count_values[i]);
    }
  }
}

// Fallback implementation for non-numeric data types to make the compiler happy (see above).
template <typename ColumnDataType>
std::enable_if_t<!std::is_arithmetic_v<ColumnDataType>, void> compute_averages(
    const std::vector<ColumnDataType>& sum_values, const std::vector<int64_t>& count_values,
    std::vector<double>& avg_values) {
  Fail("Invalid aggregate");
}

void JitAggregate::after_query(Table& out_table, JitRuntimeContext& context) const {
  auto num_columns = _aggregate_columns.size() + _groupby_columns.size();
  Segments segments(num_columns);

  // If the operator did not consume a single tuple and there are no groupby columns, we create a single row in the
  // output with uninitialized aggregate values (0 for count, NULL for sum, max, min, avg).
  if (context.hashmap.indices.empty() && _groupby_columns.empty()) {
    std::vector<AllTypeVariant> values;
    values.reserve(_aggregate_columns.size());

    for (const auto& column : _aggregate_columns) {
      if (column.function == AggregateFunction::Count) {
        values.emplace_back(int64_t{0});
      } else {
        values.emplace_back(NullValue{});
      }
    }

    out_table.append(values);
    return;
  }

  // Add each groupby column to the output table
  for (const auto& column : _groupby_columns) {
    const auto data_type = column.hashmap_entry.data_type;

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      // Get the std::vector containing the raw values (and conditionally, also get the is_null values).
      // We then create a ValueSegment of the appropriate data type from these values. Since value segments use a
      // pmr_concurrent_vector internally, this operation copies all values to a new vector of this type.
      // However, using this type of vector within the operator in the first place is also suboptimal, since the
      // pmr_concurrent_vector performs a lot of synchronization. It is thus better to use the faster std::vector and
      // perform a single copy in the end.
      auto& values = context.hashmap.columns[column.hashmap_entry.column_index].template get_vector<ColumnDataType>();
      if (column.hashmap_entry.guaranteed_non_null) {
        segments[column.position_in_table] = std::make_shared<ValueSegment<ColumnDataType>>(values);
      } else {
        auto& null_values = context.hashmap.columns[column.hashmap_entry.column_index].get_is_null_vector();
        segments[column.position_in_table] = std::make_shared<ValueSegment<ColumnDataType>>(values, null_values);
      }
    });
  }

  for (const auto& column : _aggregate_columns) {
    const auto data_type = column.hashmap_entry.data_type;

    if (column.function == AggregateFunction::Avg) {
      // Perform the post-processing for average aggregates.
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        // First get the vectors for both internal aggregates (SUM and COUNT).
        DebugAssert(column.hashmap_count_for_avg, "Invalid avg aggregate column.");
        auto sum_column_index = column.hashmap_entry.column_index;
        auto& sum_values = context.hashmap.columns[sum_column_index].template get_vector<ColumnDataType>();
        auto count_column_index = column.hashmap_count_for_avg.value().column_index;
        auto& count_values = context.hashmap.columns[count_column_index].get_vector<int64_t>();

        // Then compute the averages.
        auto avg_values = std::vector<double>(sum_values.size());
        compute_averages(sum_values, count_values, avg_values);

        if (column.hashmap_entry.guaranteed_non_null) {
          segments[column.position_in_table] = std::make_shared<ValueSegment<double>>(avg_values);
        } else {
          auto& null_values = context.hashmap.columns[column.hashmap_entry.column_index].get_is_null_vector();
          segments[column.position_in_table] = std::make_shared<ValueSegment<double>>(avg_values, null_values);
        }
      });
    } else {
      // All other types of aggregate vectors can be handled just like value segments.
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        auto column_index = column.hashmap_entry.column_index;
        auto& values = context.hashmap.columns[column_index].template get_vector<ColumnDataType>();
        if (column.hashmap_entry.guaranteed_non_null) {
          segments[column.position_in_table] = std::make_shared<ValueSegment<ColumnDataType>>(values);
        } else {
          auto& null_values = context.hashmap.columns[column.hashmap_entry.column_index].get_is_null_vector();
          segments[column.position_in_table] = std::make_shared<ValueSegment<ColumnDataType>>(values, null_values);
        }
      });
    }
  }

  out_table.append_chunk(segments);
}

namespace {

// The intermediary result of sum is always stored in a 64 bit data type. The same behaviour is implemented by non-jit
// operators.
DataType get_sum_data_type(const DataType data_type) {
  switch (data_type) {
    case DataType::Int:
      return DataType::Long;
    case DataType::Float:
      return DataType::Double;
    default:
      return data_type;
  }
}

}  // namespace

void JitAggregate::add_aggregate_column(const std::string& column_name, const JitTupleEntry& tuple_entry,
                                        const AggregateFunction function) {
  auto column_position = _aggregate_columns.size() + _groupby_columns.size();

  switch (function) {
    case AggregateFunction::Count:
      // Count aggregates always produce a non-nullable long column.
      _aggregate_columns.emplace_back(
          JitAggregateColumn{column_name, column_position, function, tuple_entry,
                             JitHashmapEntry(DataType::Long, true, _num_hashmap_columns++)});
      break;
    case AggregateFunction::Sum:
      DebugAssert(tuple_entry.data_type != DataType::String, "Invalid data type string for aggregate function sum.");
      [[fallthrough]];
    case AggregateFunction::Max:
    case AggregateFunction::Min: {
      DebugAssert(tuple_entry.data_type != DataType::Null, "Invalid data type null for aggregate function.");
      // The data type depends on the input value.
      const auto data_type =
          function == AggregateFunction::Sum ? get_sum_data_type(tuple_entry.data_type) : tuple_entry.data_type;
      _aggregate_columns.emplace_back(JitAggregateColumn{column_name, column_position, function, tuple_entry,
                                                         JitHashmapEntry(data_type, false, _num_hashmap_columns++)});
      break;
    }
    case AggregateFunction::Avg:
      DebugAssert(tuple_entry.data_type != DataType::String && tuple_entry.data_type != DataType::Null,
                  "Invalid data type for aggregate function average.");
      // Average aggregates are computed by first computing two aggregates: a SUM and a COUNT
      _aggregate_columns.emplace_back(
          JitAggregateColumn{column_name, column_position, function, tuple_entry,
                             JitHashmapEntry(get_sum_data_type(tuple_entry.data_type), false, _num_hashmap_columns++),
                             JitHashmapEntry(DataType::Long, true, _num_hashmap_columns++)});
      break;
    case AggregateFunction::CountDistinct:
      Fail("Aggregate function count distinct not supported");
    case AggregateFunction::StandardDeviationSample:
      Fail("Aggregate function standard deviation sample is not supported.");
  }
}

void JitAggregate::add_groupby_column(const std::string& column_name, const JitTupleEntry& tuple_entry) {
  auto column_position = _aggregate_columns.size() + _groupby_columns.size();
  _groupby_columns.emplace_back(JitGroupByColumn{
      column_name, column_position, tuple_entry,
      JitHashmapEntry(tuple_entry.data_type, tuple_entry.guaranteed_non_null, _num_hashmap_columns++)});
}

const std::vector<JitAggregateColumn> JitAggregate::aggregate_columns() const { return _aggregate_columns; }

const std::vector<JitGroupByColumn> JitAggregate::groupby_columns() const { return _groupby_columns; }

void JitAggregate::_consume(JitRuntimeContext& context) const {
  // We use index-based for loops in this function, since the LLVM optimizer is not able to properly unroll range-based
  // loops, and we need the unrolling for proper specialization.

  const auto num_groupby_columns = _groupby_columns.size();
  const auto num_aggregate_columns = _aggregate_columns.size();

  // Step 1: Compute hash value of the input tuple
  uint64_t hash_value = 0;

  // Compute a hash for each groupby column and combine the resulting hashes.
  for (uint32_t i = 0; i < num_groupby_columns; ++i) {
    hash_value = (hash_value << 5u) ^ jit_hash(_groupby_columns[i].tuple_entry, context);
  }

  // Step 2: Look up the rows with this hash in the hashmap.
  auto& hash_bucket = context.hashmap.indices[hash_value];

  bool found_match = false;

  // row_index will be set later on, but it needs to be initialized. Setting it to an invalid value so that we might be
  // able to catch cases where it is not set.
  uint64_t row_index{std::numeric_limits<uint64_t>::max()};

  // Iterate over each row that produces this hash. Unless there is a hash collision, there should only be at most one
  // entry for each hash. We do not need an index-based for loop here, since the number of items in each hash bucket
  // depends on runtime hash collisions and the loop is thus not specializable (i.e., not unrollable).
  for (const auto& index : hash_bucket) {
    // Compare all values of the row to the currently consumed tuple.
    bool all_values_equal = true;
    for (uint32_t i = 0; i < num_groupby_columns; ++i) {
      if (!jit_aggregate_equals(_groupby_columns[i].tuple_entry, _groupby_columns[i].hashmap_entry, index, context)) {
        all_values_equal = false;
        break;
      }
    }
    // If all values match, we have identified the group the current tuple belongs to.
    if (all_values_equal) {
      found_match = true;
      row_index = index;
      break;
    }
  }

  // If no row matches, a new tuple group must be created.
  // This requires adding a row to each output vector.
  // For groupby columns, this new row contains the value from the current tuple.
  // Aggregates are initialized with a value that is specific to their aggregate function.
  // The it_grow_by_one function appends an element to the end of an output vector and returns the index of that newly
  // added value in the vector.
  if (!found_match) {
    for (uint32_t i = 0; i < num_groupby_columns; ++i) {
      // Grow each groupby column vector and copy the value from the current tuple.
      row_index = jit_grow_by_one(_groupby_columns[i].hashmap_entry, JitVariantVector::InitialValue::Zero, context);
      jit_assign(_groupby_columns[i].tuple_entry, _groupby_columns[i].hashmap_entry, row_index, context);
    }
    for (uint32_t i = 0; i < num_aggregate_columns; ++i) {
      // Grow each aggregate column vector and initialize the aggregate with a proper initial value.
      switch (_aggregate_columns[i].function) {
        case AggregateFunction::Count:
        case AggregateFunction::Sum:
          row_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_entry, JitVariantVector::InitialValue::Zero, context);
          break;
        case AggregateFunction::Max:
          row_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_entry, JitVariantVector::InitialValue::MinValue, context);
          break;
        case AggregateFunction::Min:
          row_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_entry, JitVariantVector::InitialValue::MaxValue, context);
          break;
        case AggregateFunction::Avg:
          row_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_entry, JitVariantVector::InitialValue::Zero, context);
          DebugAssert(_aggregate_columns[i].hashmap_count_for_avg, "Invalid avg aggregate column.");
          jit_grow_by_one(_aggregate_columns[i].hashmap_count_for_avg.value(), JitVariantVector::InitialValue::Zero,
                          context);
          break;
        case AggregateFunction::CountDistinct: {
          Fail("Aggregate function count distinct not supported");
        }
        case AggregateFunction::StandardDeviationSample:
          Fail("Aggregate function standard deviation sample is not supported.");
      }
    }

    // Add the index of the new tuple group to the hashmap.
    hash_bucket.emplace_back(row_index);
  }

  // Step 3: Update the aggregate values by calling jit_aggregate_compute with appropriate operation lambdas.
  for (uint32_t i = 0; i < num_aggregate_columns; ++i) {
    switch (_aggregate_columns[i].function) {
      case AggregateFunction::Count:
        jit_aggregate_compute(jit_increment, _aggregate_columns[i].tuple_entry, _aggregate_columns[i].hashmap_entry,
                              row_index, context);
        break;
      case AggregateFunction::Sum:
        jit_aggregate_compute(jit_addition, _aggregate_columns[i].tuple_entry, _aggregate_columns[i].hashmap_entry,
                              row_index, context);
        break;
      case AggregateFunction::Max:
        jit_aggregate_compute(jit_maximum, _aggregate_columns[i].tuple_entry, _aggregate_columns[i].hashmap_entry,
                              row_index, context);
        break;
      case AggregateFunction::Min:
        jit_aggregate_compute(jit_minimum, _aggregate_columns[i].tuple_entry, _aggregate_columns[i].hashmap_entry,
                              row_index, context);
        break;
      case AggregateFunction::Avg:
        // In case of an average aggregate, the two auxiliary aggregates need to be updated.
        jit_aggregate_compute(jit_addition, _aggregate_columns[i].tuple_entry, _aggregate_columns[i].hashmap_entry,
                              row_index, context);
        DebugAssert(_aggregate_columns[i].hashmap_count_for_avg, "Invalid avg aggregate column.");
        jit_aggregate_compute(jit_increment, _aggregate_columns[i].tuple_entry,
                              _aggregate_columns[i].hashmap_count_for_avg.value(), row_index, context);
        break;
      case AggregateFunction::CountDistinct: {
        Fail("Aggregate function count distinct not supported");
      }
      case AggregateFunction::StandardDeviationSample:
        Fail("Aggregate function standard deviation sample is not supported.");
    }
  }
}

}  // namespace opossum
