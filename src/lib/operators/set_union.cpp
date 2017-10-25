#include "set_union.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

/**
 * ### SetUnion implementation
 * The SetUnion Operator turns each input table into a ReferenceMatrix.
 * The rows in the ReferenceMatrices need to be sorted in order for them to be merged, that is,
 * performing the SetUnion  operation.
 * Since sorting a multi-column matrix by rows would require a lot of value-copying, for each ReferenceMatrix, a
 * VirtualPosList is created.
 * Each element of this VirtualPosList references a row in a ReferenceMatrix by index. This way, if two values need to
 * be swapped while sorting, only two indices need to swapped instead of a RowIDs for each column in the
 * ReferenceMatrices.
 * The VirtualPosLists are sorted and then merged keeping only one element if both contain an element.
 * From the merged VirtualPosList the result table is created.
 *
 *
 * ### About ReferenceMatrices
 * The ReferenceMatrix consists of N rows and C columns of RowIDs.
 * N is the same number as the number of rows in the input table.
 * Each of the C column can represent 1..S columns in the input table. All rows represented by a ReferenceMatrix-Column
 * contain the same PosList in each Chunk.
 *
 * The ReferenceMatrix of a StoredTable will only contain one column, the ReferenceMatrix of the result of a 3 way Join
 * will contain 3 columns
 */

namespace {

// See doc above
using ReferenceMatrix = std::vector<opossum::PosList>;

// In the merged VirtualPosList, we need to identify which input side a row comes from, in order to build the output
enum class InputSide : uint8_t { Left, Right };

struct VirtualPosListEntry {
  InputSide side;
  size_t index;
};

using VirtualPosList = std::vector<VirtualPosListEntry>;

/**
 * Comparator for performing the std::set_union() of two virtual pos lists
 */
struct VirtualPosListEntryUnionContext {
  ReferenceMatrix& left;
  ReferenceMatrix& right;
  bool operator()(const VirtualPosListEntry& lhs, const VirtualPosListEntry& rhs) const {
    const auto& lhs_mpl = lhs.side == InputSide::Left ? left : right;
    const auto& rhs_mpl = rhs.side == InputSide::Left ? left : right;

    for (size_t segment_id = 0; segment_id < left.size(); ++segment_id) {
      if (lhs_mpl[segment_id][lhs.index] < rhs_mpl[segment_id][rhs.index]) return true;
      if (rhs_mpl[segment_id][rhs.index] < lhs_mpl[segment_id][lhs.index]) return false;
    }
    return false;
  }
};

/**
 * Comparator for performing the std::sort() of a virtual pos list
 */
struct VirtualPosListCmpContext {
  ReferenceMatrix& multi_pos_list;
  bool operator()(const VirtualPosListEntry& lhs, const VirtualPosListEntry& rhs) const {
    for (size_t segment_id = 0; segment_id < multi_pos_list.size(); ++segment_id) {
      const auto& segment_pos_list = multi_pos_list[segment_id];
      const auto left_row_id = segment_pos_list[lhs.index];
      const auto right_row_id = segment_pos_list[rhs.index];

      if (left_row_id < right_row_id) return true;
      if (right_row_id < left_row_id) return false;
    }
    return false;
  }
};

}  // namespace

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
  const auto early_result = _analyze_input();
  if (early_result) {
    return early_result;
  }

  /**
   * For each input, create a ReferenceMatrix
   */
  const auto build_multi_pos_list = [&](const auto& input_table, auto& multi_pos_list) {
    multi_pos_list.resize(_column_segment_begins.size());
    for (auto& pos_list : multi_pos_list) {
      pos_list.reserve(input_table->row_count());
    }

    for (auto chunk_id = ChunkID{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
      const auto& chunk = input_table->get_chunk(ChunkID{chunk_id});

      for (size_t segment_id = 0; segment_id < _column_segment_begins.size(); ++segment_id) {
        const auto column_id = _column_segment_begins[segment_id];
        const auto column = chunk.get_column(column_id);
        const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);

        auto& out_pos_list = multi_pos_list[segment_id];
        auto in_pos_list = ref_column->pos_list();
        std::copy(in_pos_list->begin(), in_pos_list->end(), std::back_inserter(out_pos_list));
      }
    }
  };

  ReferenceMatrix multi_pos_list_left;
  ReferenceMatrix multi_pos_list_right;

  build_multi_pos_list(_input_table_left(), multi_pos_list_left);
  build_multi_pos_list(_input_table_right(), multi_pos_list_right);

  /**
   * Init the virtual pos lists
   */
  const auto init_virtual_pos_list = [](const auto& table, InputSide side) {
    VirtualPosList virtual_pos_list;
    virtual_pos_list.reserve(table->row_count());

    for (size_t row_idx = 0; row_idx < table->row_count(); ++row_idx) {
      virtual_pos_list.emplace_back(VirtualPosListEntry{side, row_idx});
    }

    return virtual_pos_list;
  };

  auto virtual_pos_list_left = init_virtual_pos_list(_input_table_left(), InputSide::Left);
  auto virtual_pos_list_right = init_virtual_pos_list(_input_table_right(), InputSide::Right);

  /**
   * This is where the magic happens:
   * Compute the actual union by sorting the virtual pos lists and then merging them using std::set_union
   */
  VirtualPosListEntryUnionContext context{multi_pos_list_left, multi_pos_list_right};

  std::sort(virtual_pos_list_left.begin(), virtual_pos_list_left.end(), VirtualPosListCmpContext{multi_pos_list_left});
  std::sort(virtual_pos_list_right.begin(), virtual_pos_list_right.end(), VirtualPosListCmpContext{multi_pos_list_right});

  VirtualPosList merged_rows;
  // Min of both row counts is safe to reserve
  merged_rows.reserve(std::min(_input_table_left()->row_count(), _input_table_right()->row_count()));

  std::set_union(virtual_pos_list_left.begin(), virtual_pos_list_left.end(), virtual_pos_list_right.begin(),
                 virtual_pos_list_right.end(), std::back_inserter(merged_rows), context);

  /**
   * Build result table
   */
  const auto out_chunk_size = std::max(_input_table_left()->chunk_size(), _input_table_right()->chunk_size());
  // Actual number of rows per chunk, whereas out_chunk_size might be 0 to indicate "unlimited"
  const auto num_rows_per_chunk = out_chunk_size == 0 ? merged_rows.size() : out_chunk_size;
  auto out_table = Table::create_with_layout_from(_input_table_left(), out_chunk_size);
  for (size_t row_idx = 0; row_idx < merged_rows.size(); row_idx += num_rows_per_chunk) {
    Chunk out_chunk;

    for (size_t segment_id = 0; segment_id < _column_segment_begins.size(); segment_id++) {
      /**
       * For each segment, create a pos list
       */
      auto segment_pos_list = std::make_shared<PosList>();
      segment_pos_list->reserve(num_rows_per_chunk);
      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < num_rows_per_chunk; ++chunk_offset) {
        auto& virtual_pos_list_entry = merged_rows[row_idx + chunk_offset];
        RowID row_id;
        if (virtual_pos_list_entry.side == InputSide::Left) {
          row_id = multi_pos_list_left[segment_id][virtual_pos_list_entry.index];
        } else {
          row_id = multi_pos_list_right[segment_id][virtual_pos_list_entry.index];
        }

        segment_pos_list->emplace_back(row_id);
      }

      /**
       * Create the output columns belonging to this segment
       */
      const auto segment_column_id_begin = _column_segment_begins[segment_id];
      const auto segment_column_id_end = segment_id >= _column_segment_begins.size() - 1
                                             ? _input_table_left()->column_count()
                                             : _column_segment_begins[segment_id + 1];
      for (auto column_id = segment_column_id_begin; column_id < segment_column_id_end; ++column_id) {
        auto ref_column = std::make_shared<ReferenceColumn>(_referenced_tables[segment_id],
                                                            _referenced_column_ids[column_id], segment_pos_list);
        out_chunk.add_column(ref_column);
      }
    }

    out_table->emplace_chunk(std::move(out_chunk));
  }

  return out_table;
}

std::shared_ptr<const Table> SetUnion::_analyze_input() {
  Assert(_input_table_left()->column_count() == _input_table_right()->column_count(),
         "Input tables must have the same layout. Column count mismatch.");

  // Later code relies on input tables containing columns
  if (_input_table_left()->column_count() == 0) {
    return _input_table_left();
  }

  /**
   * Check the column layout (column names and column types)
   */
  for (ColumnID::base_type column_idx = 0; column_idx < _input_table_left()->column_count(); ++column_idx) {
    Assert(_input_table_left()->column_type(ColumnID{column_idx}) ==
               _input_table_right()->column_type(ColumnID{column_idx}),
           "Input tables must have the same layout. Column type mismatch.");
    Assert(_input_table_left()->column_name(ColumnID{column_idx}) ==
               _input_table_right()->column_name(ColumnID{column_idx}),
           "Input tables must have the same layout. Column name mismatch.");
  }

  /**
   * Later code relies on both tables having > 0 rows. If one doesn't, we can just return the other as the result of
   * the operator
   */
  if (_input_table_left()->row_count() == 0) {
    return _input_table_right();
  }
  if (_input_table_right()->row_count() == 0) {
    return _input_table_left();
  }

  /**
   * Both tables must contain only ReferenceColumns
   */
  Assert(_input_table_left()->get_type() == TableType::References &&
             _input_table_right()->get_type() == TableType::References,
         "SetUnion doesn't support non-reference tables yet");

  /**
   * Identify the column segments (verification that this is the same for all chunks happens in the #if IS_DEBUG block
   * below)
   */
  const auto add_column_segments = [&](const auto& table) {
    auto current_pos_list = std::shared_ptr<const PosList>();
    const auto& first_chunk = table->get_chunk(ChunkID{0});
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      const auto column = first_chunk.get_column(column_id);
      const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
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
  const auto unique_end_iter = std::unique(_column_segment_begins.begin(), _column_segment_begins.end());
  _column_segment_begins.resize(std::distance(_column_segment_begins.begin(), unique_end_iter));

  /**
   * Identify the tables referenced in each column segment (verification that this is the same for all chunks happens
   * in the #if IS_DEBUG block below)
   */
  const auto& first_chunk_left = _input_table_left()->get_chunk(ChunkID{0});
  for (const auto& segment_begin : _column_segment_begins) {
    const auto column = first_chunk_left.get_column(segment_begin);
    const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
    _referenced_tables.emplace_back(ref_column->referenced_table());
  }

  /**
   * Identify the column_ids referenced by each column (verification that this is the same for all chunks happens
   * in the #if IS_DEBUG block below)
   */
  for (auto column_id = ColumnID{0}; column_id < _input_table_left()->column_count(); ++column_id) {
    const auto column = first_chunk_left.get_column(column_id);
    const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
    _referenced_column_ids.emplace_back(ref_column->referenced_column_id());
  }

#if IS_DEBUG
  /**
   * Make sure all chunks have the same column segments and actually reference the tables and column_ids that the
   * segments in the first chunk of the left input table reference
   */
  const auto verify_column_segments_in_all_chunks = [&](const auto& table) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      auto current_pos_list = std::shared_ptr<const PosList>();
      size_t next_segment_id = 0;
      const auto& chunk = table->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
        if (column_id == _column_segment_begins[next_segment_id]) {
          next_segment_id++;
          current_pos_list = nullptr;
        }

        const auto column = chunk.get_column(column_id);
        const auto ref_column = std::dynamic_pointer_cast<const ReferenceColumn>(column);
        auto pos_list = ref_column->pos_list();

        if (current_pos_list == nullptr) {
          current_pos_list = pos_list;
        }

        Assert(ref_column->referenced_table() == _referenced_tables[next_segment_id - 1],
               "ReferenceColumn (Chunk: " + std::to_string(chunk_id) + ", Column: " + std::to_string(column_id) +
                   ") "
                   "doesn't reference the same table as the column at the same index in the first chunk "
                   "of the left input table does");
        Assert(ref_column->referenced_column_id() == _referenced_column_ids[column_id],
               "ReferenceColumn (Chunk: " + std::to_string(chunk_id) + ", Column: " + std::to_string(column_id) +
                   ")"
                   " doesn't reference the same table as the column at the same index in the first chunk "
                   "of the left input table does");
        Assert(current_pos_list == pos_list, "Different PosLists in column segment");
      }
    }
  };

  verify_column_segments_in_all_chunks(_input_table_left());
  verify_column_segments_in_all_chunks(_input_table_right());
#endif

  return nullptr;
}
}  // namespace opossum
