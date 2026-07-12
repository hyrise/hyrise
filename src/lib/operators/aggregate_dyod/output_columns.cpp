#include "operators/aggregate_dyod/output_columns.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

template <typename T>
TypedOutputColumn<T>::TypedOutputColumn(const bool nullable, const size_t reserve_hint) : _reserve_hint{reserve_hint} {
  _values.reserve(_reserve_hint);
  if (nullable) {
    _null_values.emplace();
    _null_values->reserve(_reserve_hint);
  }
}

template <typename T>
void TypedOutputColumn<T>::append(const T& value) {
  _values.emplace_back(value);
  if (_null_values) {
    _null_values->emplace_back(false);
  }
}

template <typename T>
void TypedOutputColumn<T>::append_null() {
  DebugAssert(_null_values, "append_null called on a non-nullable output column.");
  _values.emplace_back();
  _null_values->emplace_back(true);
}

template <typename T>
void TypedOutputColumn<T>::seal_in_progress() {
  if (_null_values) {
    _sealed.emplace_back(std::make_shared<ValueSegment<T>>(std::move(_values), std::move(*_null_values)));
    *_null_values = pmr_vector<bool>{};
    _null_values->reserve(_reserve_hint);
  } else {
    _sealed.emplace_back(std::make_shared<ValueSegment<T>>(std::move(_values)));
  }
  _values = pmr_vector<T>{};
  _values.reserve(_reserve_hint);
}

template <typename T>
size_t TypedOutputColumn<T>::in_progress_row_count() const {
  return _values.size();
}

template <typename T>
std::vector<std::shared_ptr<AbstractSegment>> TypedOutputColumn<T>::take_segments() {
  return std::move(_sealed);
}

template <typename T>
size_t TypedOutputColumn<T>::sealed_chunk_count() const {
  return _sealed.size();
}

template class TypedOutputColumn<int32_t>;
template class TypedOutputColumn<int64_t>;
template class TypedOutputColumn<float>;
template class TypedOutputColumn<double>;
template class TypedOutputColumn<pmr_string>;

OutputColumns::OutputColumns(const TableColumnDefinitions& output_column_definitions, const size_t seal_threshold)
    : _seal_threshold{seal_threshold} {
  _columns.reserve(output_column_definitions.size());
  for (const auto& definition : output_column_definitions) {
    resolve_data_type(definition.data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename std::decay_t<decltype(data_type_t)>::type;
      _columns.emplace_back(std::make_unique<TypedOutputColumn<ColumnDataType>>(definition.nullable, seal_threshold));
    });
  }
}

AbstractOutputColumn& OutputColumns::column(const size_t output_column_index) {
  return *_columns[output_column_index];
}

void OutputColumns::maybe_seal() {
  if (_columns.empty() || _columns.front()->in_progress_row_count() < _seal_threshold) {
    return;
  }
  for (auto& column : _columns) {
    column->seal_in_progress();
  }
}

void OutputColumns::seal_all() {
  if (_columns.empty() || _columns.front()->in_progress_row_count() == 0) {
    return;
  }
  for (auto& column : _columns) {
    column->seal_in_progress();
  }
}

std::shared_ptr<Table> build_output_table(const TableColumnDefinitions& output_column_definitions,
                                          std::span<OutputColumns> per_worker_outputs) {
  Assert(!output_column_definitions.empty(), "An aggregate result has at least one output column.");
  auto table = std::make_shared<Table>(output_column_definitions, TableType::Data);
  const auto column_count = output_column_definitions.size();

  for (auto& worker_output : per_worker_outputs) {
    auto segments_per_column = std::vector<std::vector<std::shared_ptr<AbstractSegment>>>{};
    segments_per_column.reserve(column_count);
    for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
      segments_per_column.emplace_back(worker_output.column(column_index).take_segments());
    }

    const auto chunk_count = segments_per_column.front().size();
    for (auto chunk_index = size_t{0}; chunk_index < chunk_count; ++chunk_index) {
      auto segments = Segments{};
      segments.reserve(column_count);
      for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
        segments.emplace_back(std::move(segments_per_column[column_index][chunk_index]));
      }
      table->append_chunk(segments);
    }
  }
  return table;
}

}  // namespace hyrise
