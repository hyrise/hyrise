#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "projection.hpp"
#include "storage/reference_column.hpp"

namespace opossum {
Projection::Projection(const std::shared_ptr<const AbstractOperator> in, const std::vector<std::string>& columns)
    : AbstractOperator(in), _column_filter(columns), _output(std::make_shared<Table>()) {}

const std::string Projection::name() const { return "Projection"; }

uint8_t Projection::num_in_tables() const { return 1; }

uint8_t Projection::num_out_tables() const { return 1; }

void Projection::execute() {
  // 1. Create a ReferenceColumn for every column in the filter
  // 2. If that column is a ReferenceColumn itself, we can simply copy its position list
  // 3. If it is a Value- or DictionaryColumn, we create a PosList that points to all entries in the table. This PosList
  // includes every entry in the table, meaning that it goes from 0 to _input_left.size(). For that reason, it can be
  // shared amongst all Value-/DictionaryColumns.

  // Needed for step 3, but not filled yet because we might not need it
  std::shared_ptr<PosList> contiguous_pos_list;

  for (ChunkID chunk_id = 0; chunk_id < _input_left->chunk_count(); chunk_id++) {
    const Chunk& chunk_in = _input_left->get_chunk(chunk_id);
    Chunk chunk_out;

    // 1. Create a ReferenceColumn for every column in the filter
    for (const auto& column_name : _column_filter) {
      auto column_id = _input_left->column_id_by_name(column_name);
      std::shared_ptr<ReferenceColumn> new_reference_column;

      if (auto referenced_col = std::dynamic_pointer_cast<ReferenceColumn>(chunk_in.get_column(column_id))) {
        // 2. If that column is a ReferenceColumn itself, we can simply copy its position list
        new_reference_column = std::make_shared<ReferenceColumn>(
            referenced_col->referenced_table(), referenced_col->referenced_column_id(), referenced_col->pos_list());
      } else {
        // 3. If it is a Value- or DictionaryColumn, we create a PosList that points to all entries in the table. This
        // PosList includes every entry in the table, meaning that it goes from 0 to _input_left.size(). For that
        // reason,
        // it can be shared amongst all Value-/DictionaryColumns. We actually had a version before where a special flag
        // simply signaled that all rows were included. This turned out to be very messy in the operator implementation.
        contiguous_pos_list = std::make_shared<PosList>(chunk_in.size());

        // yay for obscure functions. Fill the contiguous_pos_list with values from 0 to _input_left.size()
        std::iota(contiguous_pos_list->begin(), contiguous_pos_list->end(), _input_left->calculate_row_id(chunk_id, 0));
        new_reference_column = std::make_shared<ReferenceColumn>(_input_left, column_id, contiguous_pos_list);
      }

      if (chunk_id == 0) _output->add_column(column_name, _input_left->column_type(column_id), false);
      chunk_out.add_column(new_reference_column);
    }
    _output->add_chunk(std::move(chunk_out));
  }
}

std::shared_ptr<const Table> Projection::get_output() const { return _output; }
}  // namespace opossum
