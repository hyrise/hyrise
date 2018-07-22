#include "sort.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/column_iterables/chunk_offset_mapping.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

Sort::Sort(const std::shared_ptr<const AbstractOperator> in, const ColumnID column_id, const OrderByMode order_by_mode,
           const size_t output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _column_id(column_id),
      _order_by_mode(order_by_mode),
      _output_chunk_size(output_chunk_size) {}

ColumnID Sort::column_id() const { return _column_id; }

OrderByMode Sort::order_by_mode() const { return _order_by_mode; }

const std::string Sort::name() const { return "Sort"; }

std::shared_ptr<AbstractOperator> Sort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Sort>(copied_input_left, _column_id, _order_by_mode, _output_chunk_size);
}

void Sort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Sort::_on_execute() {
  _impl = make_unique_by_data_type<AbstractReadOnlyOperatorImpl, SortImpl>(
      input_table_left()->column_data_type(_column_id), input_table_left(), _column_id, _order_by_mode,
      _output_chunk_size);
  return _impl->_on_execute();
}

void Sort::_on_cleanup() { _impl.reset(); }

// This class fulfills only the materialization task for a sorted row_id_value_vector.
template <typename SortColumnType>
class Sort::SortImplMaterializeOutput {
 public:
  // creates a new table with reference columns
  SortImplMaterializeOutput(std::shared_ptr<const Table> in,
                            std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> id_value_map,
                            const size_t output_chunk_size)
      : _table_in(in), _output_chunk_size(output_chunk_size), _row_id_value_vector(id_value_map) {}

  std::shared_ptr<const Table> execute() {
    // First we create a new table as the output
    auto output = std::make_shared<Table>(_table_in->column_definitions(), TableType::Data, _output_chunk_size);

    // We have decided against duplicating MVCC columns in https://github.com/hyrise/hyrise/issues/408

    // After we created the output table and initialized the column structure, we can start adding values. Because the
    // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
    // copied column by column for each output row. For each column in a row we visit the input column with a reference
    // to the output column. This enables for the SortImplMaterializeOutput class to ignore the column types during the
    // copying of the values.
    const auto row_count_out = _row_id_value_vector->size();

    // Ceiling of integer division
    const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };

    const auto chunk_count_out = div_ceil(row_count_out, _output_chunk_size);

    // Vector of columns for each chunk
    std::vector<ChunkColumns> output_columns_by_chunk(chunk_count_out);

    // Materialize column-wise
    for (ColumnID column_id{0u}; column_id < output->column_count(); ++column_id) {
      const auto column_data_type = output->column_data_type(column_id);

      resolve_data_type(column_data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        // Initialize value columns
        auto columns_out = std::vector<std::shared_ptr<ValueColumn<ColumnDataType>>>(chunk_count_out);
        std::generate(columns_out.begin(), columns_out.end(),
                      []() { return std::make_shared<ValueColumn<ColumnDataType>>(true); });

        auto column_it = columns_out.begin();
        auto chunk_it = output_columns_by_chunk.begin();
        auto chunk_offset_out = 0u;
        for (auto row_index = 0u; row_index < row_count_out; ++row_index) {
          const auto [chunk_id, chunk_offset] = _row_id_value_vector->at(row_index).first;  // NOLINT

          const auto column = _table_in->get_chunk(chunk_id)->get_column(column_id);

          // Previously the value was retrieved by calling a virtual method,
          // which was just as slow as using the subscript operator.
          const auto value = (*column)[chunk_offset];
          (*column_it)->append(value);

          ++chunk_offset_out;

          // Check if value column is full
          if (chunk_offset_out >= _output_chunk_size) {
            chunk_offset_out = 0u;
            chunk_it->push_back(*column_it);
            ++column_it;
            ++chunk_it;
          }
        }

        // Last column has not been added
        if (chunk_offset_out > 0u) {
          chunk_it->push_back(*column_it);
        }
      });
    }

    for (auto& columns : output_columns_by_chunk) {
      output->append_chunk(columns);
    }

    return output;
  }

 protected:
  const std::shared_ptr<const Table> _table_in;
  const size_t _output_chunk_size;
  const std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
template <typename SortColumnType>
class Sort::SortImpl : public AbstractReadOnlyOperatorImpl {
 public:
  using RowIDValuePair = std::pair<RowID, SortColumnType>;

  SortImpl(const std::shared_ptr<const Table>& table_in, const ColumnID column_id,
           const OrderByMode order_by_mode = OrderByMode::Ascending, const size_t output_chunk_size = 0)
      : _table_in(table_in),
        _column_id(column_id),
        _order_by_mode(order_by_mode),
        _output_chunk_size(output_chunk_size) {
    // initialize a structure which can be sorted by std::sort
    _row_id_value_vector = std::make_shared<std::vector<RowIDValuePair>>();
    _null_value_rows = std::make_shared<std::vector<RowIDValuePair>>();
  }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    // 1. Prepare Sort: Creating rowid-value-Structure
    _materialize_sort_column();

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
      _sort_with_operator<std::less<>>();
    } else {
      _sort_with_operator<std::greater<>>();
    }

    // 2b. Insert null rows if necessary
    if (!_null_value_rows->empty()) {
      if (_order_by_mode == OrderByMode::AscendingNullsLast || _order_by_mode == OrderByMode::DescendingNullsLast) {
        // NULLs last
        _row_id_value_vector->insert(_row_id_value_vector->end(), _null_value_rows->begin(), _null_value_rows->end());
      } else {
        // NULLs first (default behavior)
        _row_id_value_vector->insert(_row_id_value_vector->begin(), _null_value_rows->begin(), _null_value_rows->end());
      }
    }

    // 3. Materialization of the result: We take the sorted ValueRowID Vector, create chunks fill them until they are
    // full and create the next one. Each chunk is filled row by row.
    auto materialization = std::make_shared<SortImplMaterializeOutput<SortColumnType>>(_table_in, _row_id_value_vector,
                                                                                       _output_chunk_size);
    return materialization->execute();
  }

  // completely materializes the sort column to create a vector of RowID-Value pairs
  void _materialize_sort_column() {
    auto& row_id_value_vector = *_row_id_value_vector;
    row_id_value_vector.reserve(_table_in->row_count());

    auto& null_value_rows = *_null_value_rows;

    for (ChunkID chunk_id{0}; chunk_id < _table_in->chunk_count(); ++chunk_id) {
      auto chunk = _table_in->get_chunk(chunk_id);

      auto base_column = chunk->get_column(_column_id);

      resolve_column_type<SortColumnType>(*base_column, [&](auto& typed_column) {
        auto iterable = create_iterable_from_column<SortColumnType>(typed_column);

        iterable.for_each([&](const auto& value) {
          if (value.is_null()) {
            null_value_rows.emplace_back(RowID{chunk_id, value.chunk_offset()}, SortColumnType{});
          } else {
            row_id_value_vector.emplace_back(RowID{chunk_id, value.chunk_offset()}, value.value());
          }
        });
      });
    }
  }

  template <typename Comparator>
  void _sort_with_operator() {
    Comparator comparator;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comparator](RowIDValuePair a, RowIDValuePair b) { return comparator(a.second, b.second); });
  }

  const std::shared_ptr<const Table> _table_in;

  // column to sort by
  const ColumnID _column_id;
  const OrderByMode _order_by_mode;
  // chunk size of the materialized output
  const size_t _output_chunk_size;

  std::shared_ptr<std::vector<RowIDValuePair>> _row_id_value_vector;
  std::shared_ptr<std::vector<RowIDValuePair>> _null_value_rows;
};

}  // namespace opossum
