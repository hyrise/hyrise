#include "set_union.hpp"

#include <utility>
#include <memory>
#include <string>
#include <algorithm>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace {

/**
 * Models a Row as a single linked list of RowIDs
 */
using ColumnSegmentsRow = std::vector<opossum::RowID>;

bool column_segments_row_cmp(const ColumnSegmentsRow& lhs, ColumnSegmentsRow & rhs) {
  for (size_t segment_id = 0; segment_id < lhs.size(); ++segment_id) {
    if (lhs[segment_id] < rhs[segment_id]) return true;
    if (rhs[segment_id] < lhs[segment_id]) return false;
  }
  return false;
}

}

namespace opossum {

SetUnion::SetUnion(const std::shared_ptr<const AbstractOperator>& left,
                         const std::shared_ptr<const AbstractOperator>& right)
    : AbstractReadOnlyOperator(left, right) {}

uint8_t SetUnion::num_in_tables() const { return 2; }

uint8_t SetUnion::num_out_tables() const { return 1; }

std::shared_ptr<AbstractOperator> SetUnion::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<SetUnion>(input_left()->recreate(args), input_right()->recreate(args));
}

const std::string SetUnion::name() const { return "SetUnion"; }

const std::string SetUnion::description() const { return "SetUnion"; }

std::shared_ptr<const Table> SetUnion::_on_execute() {
  _analyze_input();

  /**
   * For each input, create one pos list from all chunks
   */

  const auto add_column_segments_from_input_table = [](const auto& input_table, auto& out_column_segments) {
    for (auto chunk_id = ChunkID{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      const auto & chunk = input_table->get_chunk(ChunkID{chunk_id});

      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk.size(); ++chunk_offset) {
        ColumnSegmentsRow row;

        for (auto column_id : _column_segment_begins) {
          const auto column = chunk.get_column(column_id);
          const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
          row.emplace_back((*ref_column->pos_list())[chunk_offset]);
        }

        out_column_segments.emplace_back(row);
      }
    }
  };

  std::vector<ColumnSegmentsRow> column_segments_left;
  column_segments_left.reserve(_input_table_left()->row_count());
  std::vector<ColumnSegmentsRow> column_segments_right;
  column_segments_right.reserve(_input_table_right()->row_count());

  add_column_segments_from_input_table(_input_table_left(), column_segments_left);
  add_column_segments_from_input_table(_input_table_right(), column_segments_right);

  /**
   * This is where the magic happens:
   * Compute the actual union by sorting the pos_lists and then merging them using std::set_union
   */
  std::sort(column_segments_left.begin(), column_segments_left.end(), column_segments_row_cmp);
  std::sort(column_segments_right.begin(), column_segments_right.end(), column_segments_row_cmp);

  std::vector<ColumnSegmentsRow> merged_rows;
  merged_rows.reserve(_input_table_left()->row_count() + _input_table_right()->row_count());

  std::set_union(column_segments_left.begin(), column_segments_left.end(),
                 column_segments_right.begin(), column_segments_right.end(), std::back_inserter(merged_rows));

  /**
   * Build result table
   */
  const auto out_chunk_size = std::max(_input_table_left()->chunk_size(), _input_table_right()->chunk_size());
  const auto num_rows_per_chunk = out_chunk_size == 0 ? merged_rows.size() : out_chunk_size;
  auto out_table = Table::create_with_layout_from(_input_table_left(), out_chunk_size);
  auto current_chunk_offset = ChunkOffset{0};
  for (size_t row_idx = 0; row_idx < merged_rows.size(); ) {
    Chunk out_chunk;

    /**
     * For each segment, create a pos list and add
     */
    for (size_t segment_id = 0; segment_id < _column_segment_begins; segment_id++) {
      auto pos_list = std::make_shared<PosList>();
      pos_list->reserve(num_rows_per_chunk);

      std::copy()
    }

  }


  Chunk result_chunk;

  for (auto column_idx = ColumnID{0}; column_idx < _input_table_left()->col_count(); ++column_idx) {
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

void SetUnion::_analyze_input() {
  Assert(_input_table_left()->col_count() == _input_table_right()->col_count(),
         "Input tables must have the same layout. Column count mismatch.");

  // Later code relies on input tables containing columns
  if (_input_table_left()->col_count() == 0) {
    return; // TODO(moritz) make sure the operator returns the correct value
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
    return; // TODO(moritz) make sure the operator returns the correct value
  }

  Assert(_input_table_left()->get_type() == TableType::References &&
         _input_table_right()->get_type() == TableType::References,
         "SetUnion doesn't support non-reference tables yet");

  /**
   * Identify the column segments.
   */
  const auto add_column_segments = [](const auto & table) {
    auto current_pos_list = std::shared_ptr<PosList>();
    const auto& first_chunk = table->get_chunk(ChunkID{0});
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      const auto column = first_chunk.get_column(column_id);
      const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
      auto pos_list = ref_column->pos_list();

      if (current_pos_list != pos_list) {
        current_pos_list = pos_list;
        _column_segment_begins.emplace_back(column_id);
      }
    }
  };

  add_column_segments(_input_table_left());
  add_column_segments(_input_table_right());

  std::sort(_column_segment_begins.begin(), _column_segment_begins.end());
  std::unique(_column_segment_begins.begin(), _column_segment_begins.end());

  /**
   * Identify the tables referenced in each column segment
   */
  const auto& first_chunk_left = _input_table_left()->get_chunk(ChunkID{0});
  for (const auto& segment_begin : _column_segment_begins) {
    const auto column = first_chunk_left.get_column(segment_begin);
    const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
    _referenced_tables.emplace_back(ref_column->referenced_table());
  }


#if IS_DEBUG
  /**
   * Make sure all chunks have the same column segments and actually reference the tables that the segments in the
   * first chunk of the left input table reference
   */
  const auto verify_column_segments_in_all_chunks = [] (const auto & table) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      auto current_pos_list = std::shared_ptr<PosList>();
      size_t next_segment_id = 0;
      const auto & chunk = table->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        if (column_id == _column_segment_begins[next_segment_id]) {
          next_segment_id++;
          current_pos_list = nullptr;
        }

        const auto column = chunk.get_column(column_id);
        const auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column);
        auto pos_list = ref_column->pos_list();

        if (current_pos_list == nullptr) {
          current_pos_list = pos_list;
        }

        Assert(ref_column->referenced_table() == _referenced_tables[next_segment_id - 1],
               "ReferenceColumn doesn't reference the same table as the column at the same index in the first chunk"
                 "of the left input table does");
        Assert(current_pos_list == pos_list, "Different PosLists in column segment");
      }
    }
  };

  verify_column_segments_in_all_chunks(_input_table_left());
  verify_column_segments_in_all_chunks(_input_table_right());
#endif
}
}  // namespace opossum
