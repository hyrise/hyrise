#include "jit_aggregate.hpp"

#include "constant_mappings.hpp"
#include "operators/jit_operator/jit_operations.hpp"
#include "resolve_type.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

std::string JitAggregate::description() const {
  std::stringstream desc;
  desc << "[Aggregate] GroupBy: ";
  for (const auto& groupby_cxlumn : _groupby_cxlumns) {
    desc << groupby_cxlumn.cxlumn_name << " = x" << groupby_cxlumn.tuple_value.tuple_index() << ", ";
  }
  desc << " Aggregates: ";
  for (const auto& aggregate_cxlumn : _aggregate_cxlumns) {
    desc << aggregate_cxlumn.cxlumn_name << " = " << aggregate_function_to_string.left.at(aggregate_cxlumn.function)
         << "(x" << aggregate_cxlumn.tuple_value.tuple_index() << "), ";
  }
  return desc.str();
}

std::shared_ptr<Table> JitAggregate::create_output_table(const ChunkOffset input_table_chunk_size) const {
  auto num_cxlumns = _aggregate_cxlumns.size() + _groupby_cxlumns.size();
  TableCxlumnDefinitions cxlumn_definitions(num_cxlumns);

  // Add each groupby cxlumn to the output table
  for (const auto& cxlumn : _groupby_cxlumns) {
    const auto data_type = cxlumn.hashmap_value.data_type();
    const auto is_nullable = cxlumn.hashmap_value.is_nullable();
    cxlumn_definitions[cxlumn.position_in_table] = {cxlumn.cxlumn_name, data_type, is_nullable};
  }

  // Add each aggregate to the output table
  for (const auto& cxlumn : _aggregate_cxlumns) {
    // Computed averages are always of type double. The hashmap_value, however, is used to compute a SUM aggregate
    // (that is later used to produce averages in a post-processing step) and may thus have a different data type.
    const auto data_type =
        cxlumn.function == AggregateFunction::Avg ? DataType::Double : cxlumn.hashmap_value.data_type();
    const auto is_nullable = cxlumn.hashmap_value.is_nullable();
    cxlumn_definitions[cxlumn.position_in_table] = {cxlumn.cxlumn_name, data_type, is_nullable};
  }

  return std::make_shared<Table>(cxlumn_definitions, TableType::Data);
}

void JitAggregate::before_query(Table& out_table, JitRuntimeContext& context) const {
  // Resize the hashmap data structure.
  context.hashmap.cxlumns.resize(_num_hashmap_cxlumns);
}

// Performs the post-processing step for average aggregates.
// This operation is only possible for numeric data types, since averages on strings make no sense.
// However, the compiler does not understand that and requires an implementation for all data types.
// We thus rely on the SFINAE pattern to provide a fallback implementation that throws an exception.
// This is why this operation must be performed in a separate function.
template <typename CxlumnDataType>
typename std::enable_if<std::is_arithmetic<CxlumnDataType>::value, void>::type compute_averages(
    const std::vector<CxlumnDataType>& sum_values, const std::vector<int64_t>& count_values,
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
template <typename CxlumnDataType>
typename std::enable_if<!std::is_arithmetic<CxlumnDataType>::value, void>::type compute_averages(
    const std::vector<CxlumnDataType>& sum_values, const std::vector<int64_t>& count_values,
    std::vector<double>& avg_values) {
  Fail("Invalid aggregate");
}

void JitAggregate::after_query(Table& out_table, JitRuntimeContext& context) const {
  auto num_cxlumns = _aggregate_cxlumns.size() + _groupby_cxlumns.size();
  Segments segments(num_cxlumns);

  // If the operator did not consume a single tuple and there are no groupby cxlumns, we create a single row in the
  // output with uninitialized aggregate values (0 for count, NULL for sum, max, min, avg).
  if (context.hashmap.indices.empty() && _groupby_cxlumns.empty()) {
    std::vector<AllTypeVariant> values;
    values.reserve(_aggregate_cxlumns.size());

    for (const auto& cxlumn : _aggregate_cxlumns) {
      if (cxlumn.function == AggregateFunction::Count) {
        values.emplace_back(int64_t{0});
      } else {
        values.emplace_back(NullValue{});
      }
    }

    out_table.append(values);
    return;
  }

  // Add each groupby cxlumn to the output table
  for (const auto& cxlumn : _groupby_cxlumns) {
    const auto data_type = cxlumn.hashmap_value.data_type();

    resolve_data_type(data_type, [&](auto type) {
      using CxlumnDataType = typename decltype(type)::type;
      // Get the std::vector containing the raw values (and conditionally, also get the is_null values).
      // We then create a ValueSegment of the appropriate data type from these values. Since value segments use a
      // pmr_concurrent_vector internally, this operation copies all values to a new vector of this type.
      // However, using this type of vector within the operator in the first place is also suboptimal, since the
      // pmr_concurrent_vector performs a lot of synchronization. It is thus better to use the faster std::vector and
      // perform a single copy in the end.
      auto& values = context.hashmap.cxlumns[cxlumn.hashmap_value.cxlumn_index()].template get_vector<CxlumnDataType>();
      if (cxlumn.hashmap_value.is_nullable()) {
        auto& null_values = context.hashmap.cxlumns[cxlumn.hashmap_value.cxlumn_index()].get_is_null_vector();
        segments[cxlumn.position_in_table] = std::make_shared<ValueSegment<CxlumnDataType>>(values, null_values);
      } else {
        segments[cxlumn.position_in_table] = std::make_shared<ValueSegment<CxlumnDataType>>(values);
      }
    });
  }

  for (const auto& cxlumn : _aggregate_cxlumns) {
    const auto data_type = cxlumn.hashmap_value.data_type();

    if (cxlumn.function == AggregateFunction::Avg) {
      // Perform the post-processing for average aggregates.
      resolve_data_type(data_type, [&](auto type) {
        using CxlumnDataType = typename decltype(type)::type;

        // First get the vectors for both internal aggregates (SUM and COUNT).
        DebugAssert(cxlumn.hashmap_count_for_avg, "Invalid avg aggregate cxlumn.");
        auto sum_cxlumn_index = cxlumn.hashmap_value.cxlumn_index();
        auto& sum_values = context.hashmap.cxlumns[sum_cxlumn_index].template get_vector<CxlumnDataType>();
        auto count_cxlumn_index = cxlumn.hashmap_count_for_avg.value().cxlumn_index();
        auto& count_values = context.hashmap.cxlumns[count_cxlumn_index].get_vector<int64_t>();

        // Then compute the averages.
        auto avg_values = std::vector<double>(sum_values.size());
        compute_averages(sum_values, count_values, avg_values);

        if (cxlumn.hashmap_value.is_nullable()) {
          auto& null_values = context.hashmap.cxlumns[cxlumn.hashmap_value.cxlumn_index()].get_is_null_vector();
          segments[cxlumn.position_in_table] = std::make_shared<ValueSegment<double>>(avg_values, null_values);
        } else {
          segments[cxlumn.position_in_table] = std::make_shared<ValueSegment<double>>(avg_values);
        }
      });
    } else {
      // All other types of aggregate vectors can be handled just like value segments.
      resolve_data_type(data_type, [&](auto type) {
        using CxlumnDataType = typename decltype(type)::type;
        auto cxlumn_index = cxlumn.hashmap_value.cxlumn_index();
        auto& values = context.hashmap.cxlumns[cxlumn_index].template get_vector<CxlumnDataType>();
        if (cxlumn.hashmap_value.is_nullable()) {
          auto& null_values = context.hashmap.cxlumns[cxlumn.hashmap_value.cxlumn_index()].get_is_null_vector();
          segments[cxlumn.position_in_table] = std::make_shared<ValueSegment<CxlumnDataType>>(values, null_values);
        } else {
          segments[cxlumn.position_in_table] = std::make_shared<ValueSegment<CxlumnDataType>>(values);
        }
      });
    }
  }

  out_table.append_chunk(segments);
}

void JitAggregate::add_aggregate_cxlumn(const std::string& cxlumn_name, const JitTupleValue& value,
                                        const AggregateFunction function) {
  auto cxlumn_position = _aggregate_cxlumns.size() + _groupby_cxlumns.size();

  switch (function) {
    case AggregateFunction::Count:
      // Count aggregates always produce a non-nullable long cxlumn.
      _aggregate_cxlumns.emplace_back(
          JitAggregateCxlumn{cxlumn_name, cxlumn_position, function, value,
                             JitHashmapValue(DataType::Long, false, _num_hashmap_cxlumns++)});
      break;
    case AggregateFunction::Sum:
    case AggregateFunction::Max:
    case AggregateFunction::Min:
      Assert(value.data_type() != DataType::String && value.data_type() != DataType::Null,
             "Invalid data type for aggregate function.");
      // The data type depends on the input value.
      _aggregate_cxlumns.emplace_back(
          JitAggregateCxlumn{cxlumn_name, cxlumn_position, function, value,
                             JitHashmapValue(value.data_type(), true, _num_hashmap_cxlumns++)});
      break;
    case AggregateFunction::Avg:
      Assert(value.data_type() != DataType::String && value.data_type() != DataType::Null,
             "Invalid data type for aggregate function.");
      // Average aggregates are computed by first computing two aggregates: a SUM and a COUNT
      _aggregate_cxlumns.emplace_back(
          JitAggregateCxlumn{cxlumn_name, cxlumn_position, function, value,
                             JitHashmapValue(value.data_type(), true, _num_hashmap_cxlumns++),
                             JitHashmapValue(DataType::Long, false, _num_hashmap_cxlumns++)});
      break;
    case AggregateFunction::CountDistinct:
      Fail("Not supported");
  }
}

void JitAggregate::add_groupby_cxlumn(const std::string& cxlumn_name, const JitTupleValue& value) {
  auto cxlumn_position = _aggregate_cxlumns.size() + _groupby_cxlumns.size();
  _groupby_cxlumns.emplace_back(
      JitGroupByCxlumn{cxlumn_name, cxlumn_position, value,
                       JitHashmapValue(value.data_type(), value.is_nullable(), _num_hashmap_cxlumns++)});
}

const std::vector<JitAggregateCxlumn> JitAggregate::aggregate_cxlumns() const { return _aggregate_cxlumns; }

const std::vector<JitGroupByCxlumn> JitAggregate::groupby_cxlumns() const { return _groupby_cxlumns; }

void JitAggregate::_consume(JitRuntimeContext& context) const {
  // We use index-based for loops in this function, since the LLVM optimizer is not able to properly unroll range-based
  // loops, and we need the unrolling for proper specialization.

  const auto num_groupby_cxlumns = _groupby_cxlumns.size();
  const auto num_aggregate_cxlumns = _aggregate_cxlumns.size();

  // Step 1: Compute hash value of the input tuple
  uint64_t hash_value = 0;

  // Compute a hash for each groupby cxlumn and combine the resulting hashes.
  for (uint32_t i = 0; i < num_groupby_cxlumns; ++i) {
    hash_value = (hash_value << 5u) ^ jit_hash(_groupby_cxlumns[i].tuple_value, context);
  }

  // Step 2: Look up the rows with this hash in the hashmap.
  auto& hash_bucket = context.hashmap.indices[hash_value];

  bool found_match = false;
  uint64_t row_index;
  // Iterate over each row that produces this hash. Unless there is a hash collision, there should only be at most one
  // entry for each hash. We do not need an index-based for loop here, since the number of items in each hash bucket
  // depends on runtime hash collisions and the loop is thus not specializable (i.e., not unrollable).
  for (const auto& index : hash_bucket) {
    // Compare all values of the row to the currently consumed tuple.
    bool all_values_equal = true;
    for (uint32_t i = 0; i < num_groupby_cxlumns; ++i) {
      if (!jit_aggregate_equals(_groupby_cxlumns[i].tuple_value, _groupby_cxlumns[i].hashmap_value, index, context)) {
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
  // For groupby cxlumns, this new row contains the value from the current tuple.
  // Aggregates are initialized with a value that is specific to their aggregate function.
  // The it_grow_by_one function appends an element to the end of an output vector and returns the index of that newly
  // added value in the vector.
  if (!found_match) {
    for (uint32_t i = 0; i < num_groupby_cxlumns; ++i) {
      // Grow each groupby cxlumn vector and copy the value from the current tuple.
      row_index = jit_grow_by_one(_groupby_cxlumns[i].hashmap_value, JitVariantVector::InitialValue::Zero, context);
      jit_assign(_groupby_cxlumns[i].tuple_value, _groupby_cxlumns[i].hashmap_value, row_index, context);
    }
    for (uint32_t i = 0; i < num_aggregate_cxlumns; ++i) {
      // Grow each aggregate cxlumn vector and initialize the aggregate with a proper initial value.
      switch (_aggregate_cxlumns[i].function) {
        case AggregateFunction::Count:
        case AggregateFunction::Sum:
          row_index =
              jit_grow_by_one(_aggregate_cxlumns[i].hashmap_value, JitVariantVector::InitialValue::Zero, context);
          break;
        case AggregateFunction::Max:
          row_index =
              jit_grow_by_one(_aggregate_cxlumns[i].hashmap_value, JitVariantVector::InitialValue::MinValue, context);
          break;
        case AggregateFunction::Min:
          row_index =
              jit_grow_by_one(_aggregate_cxlumns[i].hashmap_value, JitVariantVector::InitialValue::MaxValue, context);
          break;
        case AggregateFunction::Avg:
          row_index =
              jit_grow_by_one(_aggregate_cxlumns[i].hashmap_value, JitVariantVector::InitialValue::Zero, context);
          DebugAssert(_aggregate_cxlumns[i].hashmap_count_for_avg, "Invalid avg aggregate cxlumn.");
          jit_grow_by_one(_aggregate_cxlumns[i].hashmap_count_for_avg.value(), JitVariantVector::InitialValue::Zero,
                          context);
          break;
        case AggregateFunction::CountDistinct:
          Fail("Not supported");
      }
    }

    // Add the index of the new tuple group to the hashmap.
    hash_bucket.emplace_back(row_index);
  }

  // Step 3: Update the aggregate values by calling jit_aggregate_compute with appropriate operation lambdas.
  for (uint32_t i = 0; i < num_aggregate_cxlumns; ++i) {
    switch (_aggregate_cxlumns[i].function) {
      case AggregateFunction::Count:
        jit_aggregate_compute(jit_increment, _aggregate_cxlumns[i].tuple_value, _aggregate_cxlumns[i].hashmap_value,
                              row_index, context);
        break;
      case AggregateFunction::Sum:
        jit_aggregate_compute(jit_addition, _aggregate_cxlumns[i].tuple_value, _aggregate_cxlumns[i].hashmap_value,
                              row_index, context);
        break;
      case AggregateFunction::Max:
        jit_aggregate_compute(jit_maximum, _aggregate_cxlumns[i].tuple_value, _aggregate_cxlumns[i].hashmap_value,
                              row_index, context);
        break;
      case AggregateFunction::Min:
        jit_aggregate_compute(jit_minimum, _aggregate_cxlumns[i].tuple_value, _aggregate_cxlumns[i].hashmap_value,
                              row_index, context);
        break;
      case AggregateFunction::Avg:
        // In case of an average aggregate, the two auxiliary aggregates need to be updated.
        jit_aggregate_compute(jit_addition, _aggregate_cxlumns[i].tuple_value, _aggregate_cxlumns[i].hashmap_value,
                              row_index, context);
        DebugAssert(_aggregate_cxlumns[i].hashmap_count_for_avg, "Invalid avg aggregate cxlumn.");
        jit_aggregate_compute(jit_increment, _aggregate_cxlumns[i].tuple_value,
                              _aggregate_cxlumns[i].hashmap_count_for_avg.value(), row_index, context);
        break;
      case AggregateFunction::CountDistinct:
        Fail("Not supported");
    }
  }
}

}  // namespace opossum
