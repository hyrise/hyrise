#pragma once

#include <vector>

#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/table.hpp"

namespace hyrise {

enum class OutputColumnOrder { LeftFirstRightSecond, RightFirstLeftSecond, LeftOnly, RightOnly };

/**
 *  General description
 * 
 *  This file contains helpers for writing the join output. There are several challenges when writing join outputs:
 *    - positions might need to be resolved (references to reference segments are not allowed)
 *    - PosLists might be joined when deemed beneficial (to avoid very small chunks)
 *    - no unnecessary PosLists should be created (i.e., re-use of PosLists)
 * 
 *  The general idea is to first create a column-to-PosLists mapping. Assume an input table which is the result of a
 *  join. In this case, there are two PosLists per chunk (each for one joined table) which are shared via a shared_ptr
 *  by each segment of the respective chunk. For larger join graphs, there can be many more referenced tables. The 
 *  mapping is later used to recognize when an existing PosList can be reused. The function `setup_pos_list_mapping` is
 *  responsible to create such a mapping.
 */

std::vector<std::shared_ptr<Chunk>> write_output_chunks(
    std::vector<RowIDPosList>& pos_lists_left, std::vector<RowIDPosList>& pos_lists_right,
    const std::shared_ptr<const Table>& left_input_table, const std::shared_ptr<const Table>& right_input_table,
    bool create_left_side_pos_lists_by_column, bool create_right_side_pos_lists_by_column,
    OutputColumnOrder output_column_order, bool allow_partition_merge);

}  // namespace hyrise
