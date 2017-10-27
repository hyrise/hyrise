#include "sort.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"
#include "storage/iterables/chunk_offset_mapping.hpp"
#include "storage/iterables/dictionary_column_iterable.hpp"
#include "storage/iterables/value_column_iterable.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

Sort::Sort(const std::shared_ptr<const AbstractOperator> in, const ColumnID column_id, const OrderByMode order_by_mode,
           const size_t output_chunk_size)
    : AbstractReadOnlyOperator(in),
      _column_id(column_id),
      _order_by_mode(order_by_mode),
      _output_chunk_size(output_chunk_size) {}

ColumnID Sort::column_id() const { return _column_id; }

OrderByMode Sort::order_by_mode() const { return _order_by_mode; }

const std::string Sort::name() const { return "Sort"; }

std::shared_ptr<AbstractOperator> Sort::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<Sort>(_input_left->recreate(args), _column_id, _order_by_mode, _output_chunk_size);
}

std::shared_ptr<const Table> Sort::_on_execute() {
  _impl = make_unique_by_column_type<AbstractReadOnlyOperatorImpl, SortImpl>(
      _input_table_left()->column_type(_column_id), _input_table_left(), _column_id, _order_by_mode,
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
    auto output = Table::create_with_layout_from(_table_in, _output_chunk_size);

    // After we created the output table and initialized the column structure, we can start adding values. Because the
    // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
    // copied column by column for each output row. For each column in a row we visit the input column with a reference
    // to the output column. This enables for the SortImplMaterializeOutput class to ignore the column types during the
    // copying of the values.
    ChunkID chunk_count_out;
    size_t output_row_count = _row_id_value_vector->size();
    if (_output_chunk_size) {
      chunk_count_out = output_row_count % _output_chunk_size ? (output_row_count / _output_chunk_size) + 1
                                                              : output_row_count / _output_chunk_size;
    } else {
      chunk_count_out = 1;
      _output_chunk_size = output_row_count;
    }

    auto row_index = 0u;

    for (auto chunk_id_out = ChunkID{0}; chunk_id_out < chunk_count_out; chunk_id_out++) {
      // Because we want to add the values row wise we have to save all columns temporarily before we can add them to
      // the
      // output chunk.
      auto column_vectors = std::vector<std::shared_ptr<BaseColumn>>(output->column_count());
      for (ColumnID column_id{0}; column_id < output->column_count(); column_id++) {
        auto column_type = _table_in->column_type(column_id);
        column_vectors[column_id] =
            make_shared_by_column_type<BaseColumn, ValueColumn>(column_type, _table_in->column_is_nullable(column_id));
      }

      Chunk chunk_out;
      // The last chunk might not be completely filled, so we have to check if we got out of range.
      for (ChunkOffset chunk_offset_out = 0; chunk_offset_out < _output_chunk_size && row_index < output_row_count;
           chunk_offset_out++) {
        auto row_id_in = _row_id_value_vector->at(row_index).first;
        for (ColumnID column_id{0}; column_id < output->column_count(); column_id++) {
          // The actual value is added by visiting the input column, which then calls a function on the output column to
          // copy the value
          auto column = _table_in->get_chunk(row_id_in.chunk_id).get_column(column_id);
          auto chunk_offset_in = row_id_in.chunk_offset;
          if (auto reference_column = std::dynamic_pointer_cast<const ReferenceColumn>(column)) {
            auto pos = reference_column->pos_list()->operator[](row_id_in.chunk_offset);
            column = reference_column->referenced_table()
                         ->get_chunk(pos.chunk_id)
                         .get_column(reference_column->referenced_column_id());
            chunk_offset_in = pos.chunk_offset;
          }
          column->copy_value_to_value_column(*column_vectors[column_id], chunk_offset_in);
        }
        row_index++;
      }
      for (ColumnID column_id{0}; column_id < output->column_count(); column_id++) {
        chunk_out.add_column(std::move(column_vectors[column_id]));
      }
      output->emplace_chunk(std::move(chunk_out));
    }

    return output;
  }

  std::shared_ptr<const Table> _table_in;
  size_t _output_chunk_size;
  std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
template <typename SortColumnType>
class Sort::SortImpl : public AbstractReadOnlyOperatorImpl {
 public:
  using RowIDValuePair = std::pair<RowID, SortColumnType>;

  SortImpl(const std::shared_ptr<const Table> table_in, const ColumnID column_id,
           const OrderByMode order_by_mode = OrderByMode::Ascending, const size_t output_chunk_size = 0)
      : _table_in(table_in),
        _column_id(column_id),
        _order_by_mode(order_by_mode),
        _output_chunk_size(output_chunk_size) {
    // initialize a structure which can be sorted by std::sort
    _row_id_value_vector = std::make_shared<std::vector<RowIDValuePair>>();
    _null_value_rows = std::make_shared<std::vector<RowIDValuePair>>();
  }

  std::shared_ptr<const Table> _on_execute() override {
    // 1. Prepare Sort: Creating rowid-value-Structure
    _materialize_sort_column();

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
      sort_with_operator<std::less<>>();
    } else {
      sort_with_operator<std::greater<>>();
    }

    // 2b. Insert null rows if necessary
    if (_null_value_rows->size()) {
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

    auto type_string = _table_in->column_type(_column_id);

    for (ChunkID chunk_id{0}; chunk_id < _table_in->chunk_count(); ++chunk_id) {
      auto& chunk = _table_in->get_chunk(chunk_id);

      auto base_column = chunk.get_column(_column_id);

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

  template <typename Comp>
  void sort_with_operator() {
    Comp comp;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comp](RowIDValuePair a, RowIDValuePair b) { return comp(a.second, b.second); });
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
