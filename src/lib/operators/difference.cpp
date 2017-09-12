#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "difference.hpp"
#include "storage/reference_column.hpp"
#include "utils/assert.hpp"

namespace opossum {
Difference::Difference(const std::shared_ptr<const AbstractOperator> left_in,
                       const std::shared_ptr<const AbstractOperator> right_in)
    : AbstractReadOnlyOperator(left_in, right_in) {}

const std::string Difference::name() const { return "Difference"; }

uint8_t Difference::num_in_tables() const { return 2; }

uint8_t Difference::num_out_tables() const { return 1; }

std::shared_ptr<const Table> Difference::_on_execute() {
  auto output = std::make_shared<Table>();

  // checking if input meets preconditions
  DebugAssert((_input_table_left()->col_count() == _input_table_right()->col_count()),
              "Input tables must have same number of columns");

  // copy column definition from _input_table_left() to output table
  for (ColumnID column_id{0}; column_id < _input_table_left()->col_count(); ++column_id) {
    auto &column_type = _input_table_left()->column_type(column_id);
    DebugAssert((column_type == _input_table_right()->column_type(column_id)),
                "Input tables must have same column order and column types");
    // add column definition to output table
    output->add_column_definition(_input_table_left()->column_name(column_id), column_type);
  }

  // 1. We create a set of all right input rows as concatenated strings.

  std::unordered_set<std::string> right_input_row_set(_input_table_right()->row_count());

  // Iterating over all chunks and for each chunk over all columns
  for (ChunkID chunk_id{0}; chunk_id < _input_table_right()->chunk_count(); chunk_id++) {
    const Chunk &chunk = _input_table_right()->get_chunk(chunk_id);
    // creating a temporary row representation with strings to be filled column wise
    auto string_row_vector = std::vector<std::string>(chunk.size());
    for (ColumnID column_id{0}; column_id < _input_table_right()->col_count(); column_id++) {
      const auto base_column = chunk.get_column(column_id);

      // filling the row vector with all values from this column
      for (ChunkOffset chunk_offset = 0; chunk_offset < base_column->size(); chunk_offset++) {
        base_column->write_string_representation(string_row_vector[chunk_offset], chunk_offset);
      }
    }
    // Remove duplicate rows by adding all rows to a unordered set
    right_input_row_set.insert(string_row_vector.begin(), string_row_vector.end());
  }

  // 2. Now we check for each chunk of the left input wich rows can be added to the output

  // Iterating over all chunks and for each chunk over all columns
  for (ChunkID chunk_id{0}; chunk_id < _input_table_left()->chunk_count(); chunk_id++) {
    const Chunk &in_chunk = _input_table_left()->get_chunk(chunk_id);
    Chunk out_chunk;

    // creating a map to share pos_lists (see table_scan.hpp)
    std::unordered_map<std::shared_ptr<const PosList>, std::shared_ptr<PosList>> out_pos_list_map;

    for (ColumnID column_id{0}; column_id < _input_table_left()->col_count(); column_id++) {
      const auto base_column = in_chunk.get_column(column_id);
      // temporary variables needed to create the reference column
      const auto referenced_column =
          std::dynamic_pointer_cast<ReferenceColumn>(_input_table_left()->get_chunk(chunk_id).get_column(column_id));
      auto out_column_id = column_id;
      auto out_referenced_table = _input_table_left();
      std::shared_ptr<const PosList> in_pos_list;

      if (referenced_column) {
        // if the input column was a reference column then the output column must reference the same values/objects
        out_column_id = referenced_column->referenced_column_id();
        out_referenced_table = referenced_column->referenced_table();
        in_pos_list = referenced_column->pos_list();
      }

      // automatically creates the entry if it does not exist
      std::shared_ptr<PosList> &pos_list_out = out_pos_list_map[in_pos_list];

      if (!pos_list_out) {
        pos_list_out = std::make_shared<PosList>();
      }

      // creating a ReferenceColumn for the output
      auto out_reference_column = std::make_shared<ReferenceColumn>(out_referenced_table, out_column_id, pos_list_out);
      out_chunk.add_column(out_reference_column);
    }

    // for all offsets check if the row can be added to the output
    for (ChunkOffset chunk_offset = 0; chunk_offset < in_chunk.size(); chunk_offset++) {
      // creating string represantation off the row at chunk_offset
      std::string row_string;
      for (ColumnID column_id{0}; column_id < _input_table_left()->col_count(); column_id++) {
        auto base_column = in_chunk.get_column(column_id);
        base_column->write_string_representation(row_string, chunk_offset);
      }

      // we check if the recently created row_string is contained in the left_input_row_set
      auto search = right_input_row_set.find(row_string);
      if (search == right_input_row_set.end()) {
        for (auto pos_list_pair : out_pos_list_map) {
          if (pos_list_pair.first) {
            pos_list_pair.second->emplace_back(pos_list_pair.first->at(chunk_offset));
          } else {
            pos_list_pair.second->emplace_back(RowID{chunk_id, chunk_offset});
          }
        }
      }
    }
    if (out_chunk.size() > 0) {
      output->add_chunk(std::move(out_chunk));
    }
  }

  return output;
}

}  // namespace opossum
