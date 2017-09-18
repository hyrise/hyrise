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

// This class fills the temporary structure to be sorted. Therefore the column to sort by is visited by this class, so
// the values can be copied in the temporary row_id_value vector.
template <typename SortColumnType>
class Sort::SortImplMaterializeSortColumn : public ColumnVisitable {
 public:
  SortImplMaterializeSortColumn(std::shared_ptr<const Table> in, const ColumnID column_id,
                                std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> id_value_map)
      : _table_in(in), _column_id(column_id), _row_id_value_vector(id_value_map) {}

  struct MaterializeSortColumnContext : ColumnVisitableContext {
    MaterializeSortColumnContext(ChunkID c, std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> id_value_map)
        : chunk_id(c), row_id_value_vector(id_value_map) {}

    // constructor for handle_reference_column
    MaterializeSortColumnContext(ChunkID c, std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> id_value_map,
                                 std::unique_ptr<ChunkOffsetsList> mapped_chunk_offsets)
        : chunk_id(c), row_id_value_vector(id_value_map), _mapped_chunk_offsets{std::move(mapped_chunk_offsets)} {}

    const ChunkID chunk_id;
    std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> row_id_value_vector;
    std::unique_ptr<ChunkOffsetsList> _mapped_chunk_offsets;
  };

  void execute() {
    _row_id_value_vector->reserve(_table_in->row_count());
    for (ChunkID chunk_id{0}; chunk_id < _table_in->chunk_count(); chunk_id++) {
      _table_in->get_chunk(chunk_id)
          .get_column(_column_id)
          ->visit(*this, std::make_shared<MaterializeSortColumnContext>(chunk_id, _row_id_value_vector));
    }
  }

  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<MaterializeSortColumnContext>(base_context);
    const auto &column = static_cast<ValueColumn<SortColumnType> &>(base_column);
    const auto &mapped_chunk_offsets = context->_mapped_chunk_offsets;

    auto chunk_id = context->chunk_id;
    auto row_id_value_vector = context->row_id_value_vector;

    auto iterable = ValueColumnIterable<SortColumnType>{column};
    iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto begin, auto end) {
      for (auto it = begin; it != end; ++it) {
        const auto value = *it;
        row_id_value_vector->emplace_back(RowID{chunk_id, value.chunk_offset()}, value.value());
      }
    });
  }

  void handle_reference_column(ReferenceColumn &column, std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<MaterializeSortColumnContext>(base_context);
    auto chunk_offsets_by_chunk_id = split_pos_list_by_chunk_id(*column.pos_list(), false);

    // Visit each referenced column, inspired by TableScan
    for (auto &pair : chunk_offsets_by_chunk_id) {
      const auto &referenced_chunk_id = pair.first;
      auto &mapped_chunk_offsets = pair.second;

      const auto &chunk = column.referenced_table()->get_chunk(referenced_chunk_id);
      auto referenced_column = chunk.get_column(column.referenced_column_id());

      auto mapped_chunk_offsets_ptr = std::make_unique<ChunkOffsetsList>(std::move(mapped_chunk_offsets));

      auto new_context = std::make_shared<MaterializeSortColumnContext>(context->chunk_id, context->row_id_value_vector,
                                                                        std::move(mapped_chunk_offsets_ptr));
      referenced_column->visit(*this, new_context);
    }
  }

  void handle_dictionary_column(BaseColumn &base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) override {
    auto context = std::static_pointer_cast<MaterializeSortColumnContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<SortColumnType> &>(base_column);
    const auto &mapped_chunk_offsets = context->_mapped_chunk_offsets;

    auto chunk_id = context->chunk_id;
    auto row_id_value_vector = context->row_id_value_vector;

    auto iterable = DictionaryColumnIterable<SortColumnType>{column};

    iterable.with_iterators(mapped_chunk_offsets.get(), [&](auto begin, auto end) {
      for (auto it = begin; it != end; ++it) {
        const auto value = *it;
        row_id_value_vector->emplace_back(RowID{chunk_id, value.chunk_offset()}, value.value());
      }
    });
  }

  const std::shared_ptr<const Table> _table_in;
  const ColumnID _column_id;
  std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

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
      output->add_column_definition(_table_in->column_name(column_id), _table_in->column_type(column_id));
    }

    // After we created the output table and initialized the column structure, we can start adding values. Because the
    // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
    // copied column by column for each output row. For each column in a row we visit the input column with a reference
    // to the output column. This enables for the SortImplMaterializeOutput class to ignore the column types during the
    // copying of the values.
    ChunkID chunk_count_out;
    if (_output_chunk_size) {
      chunk_count_out = _row_id_value_vector->size() % _output_chunk_size
                            ? (_row_id_value_vector->size() / _output_chunk_size) + 1
                            : _row_id_value_vector->size() / _output_chunk_size;
    } else {
      chunk_count_out = 1;
      _output_chunk_size = _row_id_value_vector->size();
    }
    auto row_index = 0u;
    for (auto chunk_id_out = ChunkID{0}; chunk_id_out < chunk_count_out; chunk_id_out++) {
      // Because we want to add the values row wise we have to save all columns temporarily before we can add them to
      // the
      // output chunk.
      auto column_vectors = std::vector<std::shared_ptr<BaseColumn>>(output->col_count());
      for (ColumnID column_id{0}; column_id < output->col_count(); column_id++) {
        auto column_type = _table_in->column_type(column_id);
        column_vectors[column_id] = make_shared_by_column_type<BaseColumn, ValueColumn>(column_type);
      }

      Chunk chunk_out;
      // The last chunk might not be completely filled, so we have to check if we got out of range.
      for (ChunkOffset chunk_offset_out = 0;
           chunk_offset_out < _output_chunk_size && row_index < _row_id_value_vector->size(); chunk_offset_out++) {
        auto row_id_in = _table_in->locate_row(_row_id_value_vector->at(row_index).first);
        for (ColumnID column_id{0}; column_id < output->col_count(); column_id++) {
          // The actual value is added by visiting the input column, wich then calls a function on the output column to
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
