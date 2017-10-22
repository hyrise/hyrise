#include "set_union.hpp"

#include <algorithm>
#include <chrono>
#include <numeric>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

/**
 * ### About ColumnSegments
 *
 * All neighbouring columns that use the same PosLists are said to belong to the same ColumnSegment. Just scanning a
 * table results in a table with just one ColumnSegment, joining tables will result in a table with multiple
 * ColumnSegments.
 *
 * The ColumnSegmentsRow is a vector that contains a RowID for each ColumnSegment of a Row. Using
 * column_segments_row_cmp we can std::sort() and std::set_union() the references in the input tables and thus
 * perform a set union on the input tables.
 *
 * TODO(anybody): If both input tables reference just one table, all ColumnSegmentsRows will just contains one
 * RowID and thus, for performance improvements, could be replaced with just a RowID. This remains to be evaluated.
 */

namespace {

// See doc above
using MultiPosList = std::vector<opossum::PosList>;

enum class InputSide : uint8_t {
  Left, Right
};

struct VirtualPosListEntry {
  InputSide side;
  size_t index;
};

using VirtualPosList = std::vector<VirtualPosListEntry>;

struct MultiPosListContext {
  MultiPosList &left;
  MultiPosList &right;
  bool operator()(const VirtualPosListEntry& lhs, const VirtualPosListEntry& rhs) const {
    const auto & lhs_mpl = lhs.side == InputSide::Left ? left : right;
    const auto & rhs_mpl = rhs.side == InputSide::Left ? left : right;

    for (size_t segment_id = 0; segment_id < left.size(); ++segment_id) {
      if (lhs_mpl[segment_id][lhs.index] < rhs_mpl[segment_id][rhs.index]) return true;
      if (rhs_mpl[segment_id][rhs.index] < lhs_mpl[segment_id][lhs.index]) return false;
    }
    return false;
  }
};

struct VirtualPosListContext {
  MultiPosList &multi_pos_list;
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

//void print_multi_pos_list(const MultiPosList& mpl) {
//  std::cout << "MultiPosList " << mpl.size() << "x" << mpl[0].size() << std::endl;
//  for (size_t y = 0; y < mpl[0].size(); y++) {
//    for (size_t x = 0; x < mpl.size(); x++) {
//      std::cout << mpl[x][y].chunk_id << ":" << mpl[x][y].chunk_offset;
//      if (x + 1 < mpl.size()) std::cout << ",";
//    }
//    std::cout << std::endl;
//  }
//}
//
//void print_virtual_pos_list(const VirtualPosList& vpl, const MultiPosListContext& context) {
//  for (size_t y = 0; y < vpl.size(); y++) {
//    std::cout << vpl[y].index << " " << (vpl[y].side == InputSide::Left ?"l":"r") << "|";
////
////    for (size_t segment_id = 0; segment_id < context.left.size(); ++segment_id) {
////      std::cout <<
////    }
//    std::cout << std::endl;
//  }
//}

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

  auto input_build_begin = std::chrono::high_resolution_clock::now();
  /**
   * For each input, create a MultiPosList
   */
  const auto build_multi_pos_list = [&](const auto& input_table, auto& multi_pos_list) {
    multi_pos_list.resize(_column_segment_begins.size());
    for (auto & pos_list : multi_pos_list) {
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

  MultiPosList multi_pos_list_left;
  MultiPosList multi_pos_list_right;

  build_multi_pos_list(_input_table_left(), multi_pos_list_left);
  build_multi_pos_list(_input_table_right(), multi_pos_list_right);

  auto input_build_begin2 = std::chrono::high_resolution_clock::now();
//  std::cout << "MultiPosListLeft:" << std::endl;
//  print_multi_pos_list(multi_pos_list_left);
//  std::cout << "MultiPosListRight:" << std::endl;
//  print_multi_pos_list(multi_pos_list_right);

  /**
   * Init the virtual pos lists
   */
  const auto init_virtual_pos_list = [] (const auto &table, InputSide side) {
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
  MultiPosListContext context{multi_pos_list_left, multi_pos_list_right};

  auto sort_begin = std::chrono::high_resolution_clock::now();
  std::sort(virtual_pos_list_left.begin(), virtual_pos_list_left.end(), VirtualPosListContext{multi_pos_list_left});
  std::sort(virtual_pos_list_right.begin(), virtual_pos_list_right.end(), VirtualPosListContext{multi_pos_list_right});

  auto sort_end = std::chrono::high_resolution_clock::now();
//  std::cout << "VirtualPosListLeft" << std::endl;
//  print_virtual_pos_list(virtual_pos_list_left, context);
//
//  std::cout << "VirtualPosListRight" << std::endl;
//  print_virtual_pos_list(virtual_pos_list_right, context);

  VirtualPosList merged_rows;
  // Min of both row counts is safe to reserve
  merged_rows.reserve(std::min(_input_table_left()->row_count(), _input_table_right()->row_count()));

  std::set_union(virtual_pos_list_left.begin(), virtual_pos_list_left.end(), virtual_pos_list_right.begin(),
                 virtual_pos_list_right.end(), std::back_inserter(merged_rows), context);
  auto union_end = std::chrono::high_resolution_clock::now();


//  std::cout << "VirtualPosListMerged" << std::endl;
//  print_virtual_pos_list(merged_rows, context);

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

  auto generation_end = std::chrono::high_resolution_clock::now();
  std::cout << "Input0: " << std::chrono::duration_cast<std::chrono::milliseconds>(input_build_begin2 - input_build_begin).count() << std::endl;
  std::cout << "Input1: " << std::chrono::duration_cast<std::chrono::milliseconds>(sort_begin - input_build_begin2).count() << std::endl;
  std::cout << "Sort: " << std::chrono::duration_cast<std::chrono::milliseconds>(sort_end - sort_begin).count() << std::endl;
  std::cout << "Union: " << std::chrono::duration_cast<std::chrono::milliseconds>(union_end - sort_end).count() << std::endl;
  std::cout << "Generation: " << std::chrono::duration_cast<std::chrono::milliseconds>(generation_end - union_end).count() << std::endl;

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
