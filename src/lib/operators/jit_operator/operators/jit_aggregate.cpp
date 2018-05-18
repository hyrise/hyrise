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
  auto num_columns = _aggregate_columns.size() + _groupby_columns.size();
  TableColumnDefinitions column_definitions(num_columns);

  for (const auto& column : _groupby_columns) {
    const auto data_type = column.hashmap_value.data_type();
    const auto is_nullable = column.hashmap_value.is_nullable();
    column_definitions[column.table_position] = {column.column_name, data_type, is_nullable};
  }

  for (const auto& column : _aggregate_columns) {
    const auto data_type =
        column.function == AggregateFunction::Avg ? DataType::Double : column.hashmap_value.data_type();
    const auto is_nullable = column.hashmap_value.is_nullable();
    column_definitions[column.table_position] = {column.column_name, data_type, is_nullable};
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
  auto num_columns = _aggregate_columns.size() + _groupby_columns.size();
  ChunkColumns chunk_columns(num_columns);

  for (const auto& column : _groupby_columns) {
    const auto data_type = column.hashmap_value.data_type();

    resolve_data_type(data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      auto& values = context.hashmap.values[column.hashmap_value.column_index()].template get_vector<ColumnDataType>();
      if (column.hashmap_value.is_nullable()) {
        auto& is_null_values = context.hashmap.values[column.hashmap_value.column_index()].get_is_null_vector();
        chunk_columns[column.table_position] = std::make_shared<ValueColumn<ColumnDataType>>(values, is_null_values);
      } else {
        chunk_columns[column.table_position] = std::make_shared<ValueColumn<ColumnDataType>>(values);
      }
    });
  }

  for (const auto& column : _aggregate_columns) {
    const auto data_type = column.hashmap_value.data_type();

    if (column.function == AggregateFunction::Avg) {
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        auto sum_column_index = column.hashmap_value.column_index();
        auto& sum_values = context.hashmap.values[sum_column_index].template get_vector<ColumnDataType>();

        DebugAssert(column.hashmap_value_2, "Invalid avg aggregate column.");
        auto count_column_index = column.hashmap_value_2.value().column_index();
        auto& count_values = context.hashmap.values[count_column_index].get_vector<int64_t>();
        auto avg_values = std::vector<double>(sum_values.size());
        _compute_averages(sum_values, count_values, avg_values);
        if (column.hashmap_value.is_nullable()) {
          auto& is_null_values = context.hashmap.values[column.hashmap_value.column_index()].get_is_null_vector();
          chunk_columns[column.table_position] = std::make_shared<ValueColumn<double>>(avg_values, is_null_values);
        } else {
          chunk_columns[column.table_position] = std::make_shared<ValueColumn<double>>(avg_values);
        }
      });
    } else {
      resolve_data_type(data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;
        auto column_index = column.hashmap_value.column_index();
        auto& values = context.hashmap.values[column_index].template get_vector<ColumnDataType>();
        if (column.hashmap_value.is_nullable()) {
          auto& is_null_values = context.hashmap.values[column.hashmap_value.column_index()].get_is_null_vector();
          chunk_columns[column.table_position] = std::make_shared<ValueColumn<ColumnDataType>>(values, is_null_values);
        } else {
          chunk_columns[column.table_position] = std::make_shared<ValueColumn<ColumnDataType>>(values);
        }
      });
    }
  }

  out_table.append_chunk(chunk_columns);
}

void JitAggregate::add_aggregate_column(const std::string& column_name, const JitTupleValue& value,
                                        const AggregateFunction function) {
  auto column_position = _aggregate_columns.size() + _groupby_columns.size();
  switch (function) {
    case AggregateFunction::Count:
      _aggregate_columns.push_back(JitAggregateColumn{column_name, column_position, function, value,
                                                      JitHashmapValue(DataType::Long, false, _num_hashmap_values++)});
      break;
    case AggregateFunction::Sum:
    case AggregateFunction::Max:
    case AggregateFunction::Min:
      Assert(value.data_type() != DataType::String && value.data_type() != DataType::Null,
             "Invalid data type for aggregate function.");
      _aggregate_columns.push_back(
          JitAggregateColumn{column_name, column_position, function, value,
                             JitHashmapValue(value.data_type(), value.is_nullable(), _num_hashmap_values++)});
      break;
    case AggregateFunction::Avg:
      Assert(value.data_type() != DataType::String && value.data_type() != DataType::Null,
             "Invalid data type for aggregate function.");
      _aggregate_columns.push_back(
          JitAggregateColumn{column_name, column_position, function, value,
                             JitHashmapValue(value.data_type(), value.is_nullable(), _num_hashmap_values++),
                             JitHashmapValue(DataType::Long, false, _num_hashmap_values++)});
      break;
    case AggregateFunction::CountDistinct:
      Fail("Not supported");
  }
}

void JitAggregate::add_groupby_column(const std::string& column_name, const JitTupleValue& value) {
  auto column_position = _aggregate_columns.size() + _groupby_columns.size();
  _groupby_columns.push_back(
      JitGroupByColumn{column_name, column_position, value,
                       JitHashmapValue(value.data_type(), value.is_nullable(), _num_hashmap_values++)});
}

const std::vector<JitAggregateColumn> JitAggregate::aggregate_columns() const { return _aggregate_columns; }

const std::vector<JitGroupByColumn> JitAggregate::groupby_columns() const { return _groupby_columns; }

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
          DebugAssert(_aggregate_columns[i].hashmap_value_2, "Invalid avg aggregate column.");
          jit_grow_by_one(_aggregate_columns[i].hashmap_value_2.value(), JitVariantVector::InitialValue::Zero, context);
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
        DebugAssert(_aggregate_columns[i].hashmap_value_2, "Invalid avg aggregate column.");
        jit_aggregate_compute(jit_increment, _aggregate_columns[i].tuple_value,
                              _aggregate_columns[i].hashmap_value_2.value(), match_index, context);
        break;
      case AggregateFunction::CountDistinct:
        Fail("Not supported");
    }
  }
}

}  // namespace opossum
