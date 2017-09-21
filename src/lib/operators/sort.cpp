#include "sort.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/base_attribute_vector.hpp"
#include "storage/column_visitable.hpp"
#include "storage/iterables/chunk_offset_mapping.hpp"
#include "storage/iterables/dictionary_column_iterable.hpp"
#include "storage/iterables/value_column_iterable.hpp"

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

uint8_t Sort::num_in_tables() const { return 1; }

uint8_t Sort::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> Sort::recreate(const std::vector<AllParameterVariant> &args) const {
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
    auto output = std::make_shared<Table>(_output_chunk_size);

    for (ColumnID column_id{0}; column_id < _table_in->col_count(); column_id++) {
      output->add_column_definition(_table_in->column_name(column_id), _table_in->column_type(column_id),
                                    _table_in->column_is_nullable(column_id));
    }

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
      auto column_vectors = std::vector<std::shared_ptr<BaseColumn>>(output->col_count());
      for (ColumnID column_id{0}; column_id < output->col_count(); column_id++) {
        auto column_type = _table_in->column_type(column_id);
        column_vectors[column_id] =
            make_shared_by_column_type<BaseColumn, ValueColumn>(column_type, _table_in->column_is_nullable(column_id));
      }

      Chunk chunk_out;
      // The last chunk might not be completely filled, so we have to check if we got out of range.
      for (ChunkOffset chunk_offset_out = 0; chunk_offset_out < _output_chunk_size && row_index < output_row_count;
           chunk_offset_out++) {
        auto row_id_in = _table_in->locate_row(_row_id_value_vector->at(row_index).first);
        for (ColumnID column_id{0}; column_id < output->col_count(); column_id++) {
          // The actual value is added by visiting the input column, which then calls a function on the output column to
          // copy the value
          auto column = _table_in->get_chunk(row_id_in.first).get_column(column_id);
          auto chunk_offset_in = row_id_in.second;
          if (auto reference_column = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
            auto pos = reference_column->pos_list()->operator[](row_id_in.second);
            column = reference_column->referenced_table()
                         ->get_chunk(pos.chunk_id)
                         .get_column(reference_column->referenced_column_id());
            chunk_offset_in = pos.chunk_offset;
          }
          column->copy_value_to_value_column(*column_vectors[column_id], chunk_offset_in);
        }
        row_index++;
      }
      for (ColumnID column_id{0}; column_id < output->col_count(); column_id++) {
        chunk_out.add_column(std::move(column_vectors[column_id]));
      }
      output->add_chunk(std::move(chunk_out));
    }

    return output;
  }

  std::shared_ptr<const Table> _table_in;
  size_t _output_chunk_size;
  std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

}  // namespace opossum
