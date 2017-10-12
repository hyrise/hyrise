#include "union_unique.hpp"

#include <utility>
#include <memory>
#include <string>
#include <algorithm>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"

namespace opossum {

UnionUnique::UnionUnique(const std::shared_ptr<const AbstractOperator>& left,
                         const std::shared_ptr<const AbstractOperator>& right)
    : AbstractReadOnlyOperator(left, right) {}

uint8_t UnionUnique::num_in_tables() const { return 2; }

uint8_t UnionUnique::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> UnionUnique::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<UnionUnique>(input_left()->recreate(args), input_right()->recreate(args));
}

const std::string UnionUnique::name() const { return "UnionUnique"; }

const std::string UnionUnique::description() const { return "UnionUnique"; }

std::shared_ptr<const Table> UnionUnique::_on_execute() {
  auto referenced_table = _verify_input_and_get_referenced_table();


  /**
   * Build the pos list (out_pos_list) containing all row_ids of both tables
   */
  auto out_pos_list = std::make_shared<PosList>();
  out_pos_list->reserve(_input_table_left()->row_count() + _input_table_right()->row_count());
  std::vector<ColumnID> out_column_ids(referenced_table->col_count());

  auto add_row_ids_from_input_table = [&out_pos_list](const auto& input_table) {
    for (ChunkID::base_type chunk_idx = 0; chunk_idx < input_table->chunk_count(); ++chunk_idx) {
      const auto column = input_table->get_chunk(ChunkID{chunk_idx}).get_column(ColumnID{0});
      const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
      const auto& in_pos_list = ref_column->pos_list();

      std::copy(in_pos_list->begin(), in_pos_list->end(), std::back_inserter(*out_pos_list));
    }
  };

  add_row_ids_from_input_table(_input_table_left());
  add_row_ids_from_input_table(_input_table_right());

  /**
   * This is where the magic happens:
   * Compute the actual union by sorting the pos_list and then eliminating duplicates.
   */
  std::sort(out_pos_list->begin(), out_pos_list->end());
  const auto unique_end = std::unique(out_pos_list->begin(), out_pos_list->end());

  out_pos_list->resize(static_cast<size_t>(unique_end - out_pos_list->begin()));

  /**
   * Build result table
   */
  Chunk result_chunk;

  for (ColumnID::base_type column_idx = 0; column_idx < _input_table_left()->col_count(); ++column_idx) {
    const auto in_column = _input_table_left()->get_chunk(ChunkID{0}).get_column(ColumnID{column_idx});
    const auto in_ref_column = std::dynamic_pointer_cast<ReferenceColumn>(in_column);

    auto out_column_id = in_ref_column->referenced_column_id();
    auto out_ref_column = std::make_shared<ReferenceColumn>(referenced_table, out_column_id, out_pos_list);

    result_chunk.add_column(out_ref_column);
  }

  auto result_table = Table::create_with_layout_from(_input_table_left(), 0);
  result_table->emplace_chunk(std::move(result_chunk));

  return result_table;
}

std::shared_ptr<const Table> UnionUnique::_verify_input_and_get_referenced_table() const {
  Assert(_input_table_left()->col_count() == _input_table_right()->col_count(),
         "Input tables must have the same layout. Column count mismatch.");

  // Later code relies on input tables containing columns
  if (_input_table_left()->col_count() == 0) {
    return std::make_shared<Table>();
  }

  /**
   * Check the column layout
   */
  for (ColumnID::base_type column_idx = 0; column_idx < _input_table_left()->col_count(); ++column_idx) {
    Assert(_input_table_left()->column_type(ColumnID{column_idx}) ==
           _input_table_right()->column_type(ColumnID{column_idx}),
           "Input tables must have the same layout. Column type mismatch.");
    Assert(_input_table_left()->column_name(ColumnID{column_idx}) ==
           _input_table_right()->column_name(ColumnID{column_idx}),
           "Input tables must have the same layout. Column name mismatch.");
  }

  // Later code relies on both tables having > 0 rows
  if (_input_table_left()->row_count() == 0 && _input_table_right()->row_count() == 0) {
    return _input_table_left();
  }

  Assert(_input_table_left()->get_type() == TableType::References &&
         _input_table_right()->get_type() == TableType::References,
         "UnionUnique doesn't support non-reference tables yet");

  /**
   * Identify the referenced table and whether both input tables reference the same one
   */
  const auto referenced_table_left =
    std::dynamic_pointer_cast<ReferenceColumn>(_input_table_left()->get_chunk(ChunkID{0}).get_column(ColumnID{0}))
      ->referenced_table();
  const auto referenced_table_right =
    std::dynamic_pointer_cast<ReferenceColumn>(_input_table_right()->get_chunk(ChunkID{0}).get_column(ColumnID{0}))
      ->referenced_table();

  Assert(referenced_table_left == referenced_table_right, "Input tables must reference the same table");

  const auto& referenced_table = referenced_table_left;

  /**
   * Make sure
   *    - all columns in all chunks of both tables reference the referenced_table
   *    - each chunk uses only one pos list
   */
  const auto verify_referenced_table = [&] (const auto & input_table) {
    for (auto chunk_idx = ChunkID{0}; chunk_idx < input_table->chunk_count(); ++chunk_idx) {
      auto & chunk = input_table->get_chunk(chunk_idx);
      std::shared_ptr<PosList> column_pos_list;
      for (auto column_idx = ColumnID{0}; column_idx < input_table->col_count(); ++column_idx) {
        const auto column = chunk.get_column(column_idx);
        const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
        Assert(ref_column, "Non ReferenceColumn detected in input table");
        Assert(ref_column->referenced_table() == referenced_table, "Input table references multiple tables");
        const auto pos_list = ref_column->pos_list();
        Assert(!column_pos_list || pos_list == column_pos_list, "Multiple pos lists detected in Chunk");
      }
    }
  };
  verify_referenced_table(_input_table_left());
  verify_referenced_table(_input_table_right());

  return referenced_table;
}
}  // namespace opossum
