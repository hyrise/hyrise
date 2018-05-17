#include "jit_aggregate.hpp"

#include "constant_mappings.hpp"
#include "operators/jit_operator/jit_operations.hpp"
#include "resolve_type.hpp"
#include "storage/value_column.hpp"

namespace opossum {

std::string JitAggregate::description() const {
  std::stringstream desc;
  desc << "[Aggregate] GroupBy: ";
  for (const auto& groupby_column : _groupby_columns) {
    desc << groupby_column.column_name << " = x" << groupby_column.tuple_value.tuple_index() << ", ";
  }
  desc << " Aggregates: ";
  for (const auto& aggregate_column : _aggregate_columns) {
    desc << aggregate_column.column_name << " = " << aggregate_function_to_string.left.at(aggregate_column.function)
         << "(x" << aggregate_column.tuple_value.tuple_index() << "), ";
  }
  return desc.str();
}

std::shared_ptr<Table> JitAggregate::create_output_table(const uint32_t max_chunk_size) const {
  TableColumnDefinitions column_definitions;
  for (const auto& groupby_column : _groupby_columns) {
    const auto data_type = groupby_column.hashmap_value.data_type();
    const auto is_nullable = groupby_column.hashmap_value.is_nullable();
    column_definitions.emplace_back(groupby_column.column_name, data_type, is_nullable);
  }

  for (const auto& aggregate_column : _aggregate_columns) {
    const auto data_type = aggregate_column.function == AggregateFunction::Avg
                               ? DataType::Double
                               : aggregate_column.hashmap_value.data_type();
    const auto is_nullable = aggregate_column.hashmap_value.is_nullable();
    column_definitions.emplace_back(aggregate_column.column_name, data_type, is_nullable);
  }

  return std::make_shared<Table>(column_definitions, TableType::Data, Chunk::MAX_SIZE);
}

void JitAggregate::before_query(Table& out_table, JitRuntimeContext& context) const {
  context.hashmap.values.resize(_num_hashmap_values);
}

template <typename ColumnDataType>
typename std::enable_if<std::is_arithmetic<ColumnDataType>::value, void>::type _compute_averages(
    const std::vector<ColumnDataType>& sum_values, const std::vector<int64_t>& count_values,
    std::vector<double>& avg_values) {
  for (auto i = 0u; i < sum_values.size(); ++i) {
    avg_values[i] = sum_values[i] / static_cast<double>(count_values[i]);
  }
}

template <typename ColumnDataType>
typename std::enable_if<!std::is_arithmetic<ColumnDataType>::value, void>::type _compute_averages(
    const std::vector<ColumnDataType>& sum_values, const std::vector<int64_t>& count_values,
    std::vector<double>& avg_values) {
  Fail("Invalid aggregate");
}

void JitAggregate::after_query(Table& out_table, JitRuntimeContext& context) const {
  ChunkColumns chunk_columns;
  for (const auto& groupby_column : _groupby_columns) {
    const auto data_type = groupby_column.hashmap_value.data_type();

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto& values =
          context.hashmap.values[groupby_column.hashmap_value.column_index()].template get_vector<ColumnDataType>();
      auto column = std::make_shared<ValueColumn<ColumnDataType>>(values);
      chunk_columns.push_back(column);
    });
  }

  for (const auto& aggregate_column : _aggregate_columns) {
    const auto data_type = aggregate_column.hashmap_value.data_type();

    if (aggregate_column.function == AggregateFunction::Avg) {
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        auto& sum_values =
            context.hashmap.values[aggregate_column.hashmap_value.column_index()].template get_vector<ColumnDataType>();
        auto& count_values =
            context.hashmap.values[aggregate_column.hashmap_value_2.column_index()].get_vector<int64_t>();
        auto avg_values = std::vector<double>(sum_values.size());
        _compute_averages(sum_values, count_values, avg_values);
        auto column = std::make_shared<ValueColumn<double>>(avg_values);
        chunk_columns.push_back(column);
      });
    } else {
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        auto& values =
            context.hashmap.values[aggregate_column.hashmap_value.column_index()].template get_vector<ColumnDataType>();
        auto column = std::make_shared<ValueColumn<ColumnDataType>>(values);
        chunk_columns.push_back(column);
      });
    }
  }

  out_table.append_chunk(chunk_columns);
}

void JitAggregate::add_aggregate_column(const std::string& column_name, const JitTupleValue& value,
                                        const AggregateFunction function) {
  switch (function) {
    case AggregateFunction::Count:
      _aggregate_columns.push_back(JitAggregateColumn{column_name, function, value,
                                                      JitHashmapValue(DataType::Long, false, _num_hashmap_values++),
                                                      JitHashmapValue(DataType::Null, false, -1)});
      break;
    case AggregateFunction::Sum:
    case AggregateFunction::Max:
    case AggregateFunction::Min:
      _aggregate_columns.push_back(JitAggregateColumn{column_name, function, value,
                                                      JitHashmapValue(value.data_type(), true, _num_hashmap_values++),
                                                      JitHashmapValue(DataType::Null, false, -1)});
      break;
    case AggregateFunction::Avg:
      _aggregate_columns.push_back(JitAggregateColumn{column_name, function, value,
                                                      JitHashmapValue(value.data_type(), true, _num_hashmap_values++),
                                                      JitHashmapValue(DataType::Long, false, _num_hashmap_values++)});
      break;
    case AggregateFunction::CountDistinct:
      Fail("Not supported");
  }
}

void JitAggregate::add_groupby_column(const std::string& column_name, const JitTupleValue& value) {
  _groupby_columns.push_back(JitGroupByColumn{
      column_name, value, JitHashmapValue(value.data_type(), value.is_nullable(), _num_hashmap_values++)});
}

void JitAggregate::_consume(JitRuntimeContext& context) const {
  // compute hash value
  uint64_t hash_value = 0;

  const auto num_groupby_columns = _groupby_columns.size();
  const auto num_aggregate_columns = _aggregate_columns.size();

  for (uint32_t i = 0; i < num_groupby_columns; ++i) {
    hash_value = (hash_value << 5) ^ jit_hash(_groupby_columns[i].tuple_value, context);
  }

  auto& hash_bucket = context.hashmap.indices[hash_value];

  bool found_match = false;
  uint64_t match_index;
  for (const auto& index : hash_bucket) {
    bool all_values_equal = true;
    for (uint32_t i = 0; i < num_groupby_columns; ++i) {
      if (!jit_aggregate_equals(_groupby_columns[i].tuple_value, _groupby_columns[i].hashmap_value, index, context)) {
        all_values_equal = false;
        break;
      }
    }
    if (all_values_equal) {
      found_match = true;
      match_index = index;
      break;
    }
  }

  if (!found_match) {
    for (uint32_t i = 0; i < num_groupby_columns; ++i) {
      match_index = jit_grow_by_one(_groupby_columns[i].hashmap_value, JitVariantVector::InitialValue::Zero, context);
      jit_assign(_groupby_columns[i].tuple_value, _groupby_columns[i].hashmap_value, match_index, context);
    }
    for (uint32_t i = 0; i < num_aggregate_columns; ++i) {
      switch (_aggregate_columns[i].function) {
        case AggregateFunction::Count:
        case AggregateFunction::Sum:
          match_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_value, JitVariantVector::InitialValue::Zero, context);
          break;
        case AggregateFunction::Max:
          match_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_value, JitVariantVector::InitialValue::MinValue, context);
          break;
        case AggregateFunction::Min:
          match_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_value, JitVariantVector::InitialValue::MaxValue, context);
          break;
        case AggregateFunction::Avg:
          match_index =
              jit_grow_by_one(_aggregate_columns[i].hashmap_value, JitVariantVector::InitialValue::Zero, context);
          jit_grow_by_one(_aggregate_columns[i].hashmap_value_2, JitVariantVector::InitialValue::Zero, context);
          break;
        case AggregateFunction::CountDistinct:
          Fail("Not supported");
      }
    }
    hash_bucket.push_back(match_index);
  }

  for (uint32_t i = 0; i < num_aggregate_columns; ++i) {
    switch (_aggregate_columns[i].function) {
      case AggregateFunction::Count:
        jit_aggregate_compute(jit_increment, _aggregate_columns[i].tuple_value, _aggregate_columns[i].hashmap_value,
                              match_index, context);
        break;
      case AggregateFunction::Sum:
        jit_aggregate_compute(jit_addition, _aggregate_columns[i].tuple_value, _aggregate_columns[i].hashmap_value,
                              match_index, context);
        break;
      case AggregateFunction::Max:
        jit_aggregate_compute(jit_maximum, _aggregate_columns[i].tuple_value, _aggregate_columns[i].hashmap_value,
                              match_index, context);
        break;
      case AggregateFunction::Min:
        jit_aggregate_compute(jit_minimum, _aggregate_columns[i].tuple_value, _aggregate_columns[i].hashmap_value,
                              match_index, context);
        break;
      case AggregateFunction::Avg:
        jit_aggregate_compute(jit_addition, _aggregate_columns[i].tuple_value, _aggregate_columns[i].hashmap_value,
                              match_index, context);
        jit_aggregate_compute(jit_increment, _aggregate_columns[i].tuple_value, _aggregate_columns[i].hashmap_value_2,
                              match_index, context);
        break;
      case AggregateFunction::CountDistinct:
        Fail("Not supported");
    }
  }
}

}  // namespace opossum
