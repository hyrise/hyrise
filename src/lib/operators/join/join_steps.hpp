#pragma once

#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"


namespace opossum {

using PosLists = std::vector<std::shared_ptr<const AbstractPosList>>;
using PosListsByChunk = std::vector<std::shared_ptr<PosLists>>;

/**
 * Returns a vector where each entry with index i references a PosLists object. The PosLists object
 * contains the position list of every segment/chunk in column i.
 * @param input_table
 */
// See usage in _on_execute() for doc.
inline PosListsByChunk setup_pos_lists_by_chunk(const std::shared_ptr<const Table>& input_table) {
  Assert(input_table->type() == TableType::References, "Function only works for reference tables");

  std::map<PosLists, std::shared_ptr<PosLists>> shared_pos_lists_by_pos_lists;

  PosListsByChunk pos_lists_by_segment(input_table->column_count());
  auto pos_lists_by_segment_it = pos_lists_by_segment.begin();

  const auto input_chunks_count = input_table->chunk_count();
  const auto input_columns_count = input_table->column_count();

  // For every column, for every chunk
  for (ColumnID column_id{0}; column_id < input_columns_count; ++column_id) {
    // Get all the input pos lists so that we only have to pointer cast the segments once
    auto pos_list_ptrs = std::make_shared<PosLists>(input_table->chunk_count());
    auto pos_lists_iter = pos_list_ptrs->begin();

    // Iterate over every chunk and add the chunks segment with column_id to pos_list_ptrs
    for (ChunkID chunk_id{0}; chunk_id < input_chunks_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      const auto& ref_segment_uncasted = chunk->get_segment(column_id);
      const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(ref_segment_uncasted);
      *pos_lists_iter = ref_segment->pos_list();
      ++pos_lists_iter;
    }

    // pos_list_ptrs contains all position lists of the reference segments for the column_id.
    auto iter = shared_pos_lists_by_pos_lists.emplace(*pos_list_ptrs, pos_list_ptrs).first;

    *pos_lists_by_segment_it = iter->second;
    ++pos_lists_by_segment_it;
  }

  return pos_lists_by_segment;
}


/**
 *
 * @param output_segments [in/out] Vector to which the newly created reference segments will be written.
 * @param input_table Table which all the position lists reference
 * @param input_pos_list_ptrs_sptrs_by_segments Contains all position lists to all columns of input table
 * @param pos_list contains the positions of rows to use from the input table
 */
inline void write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                  const PosListsByChunk& input_pos_list_ptrs_sptrs_by_segments,
                                  std::shared_ptr<RowIDPosList> pos_list) {
  std::map<std::shared_ptr<PosLists>, std::shared_ptr<RowIDPosList>> output_pos_list_cache;

  // We might use this later, but want to have it outside of the for loop
  std::shared_ptr<Table> dummy_table;

  // Add segments from input table to output chunk
  // for every column for every row in pos_list: get corresponding PosList of input_pos_list_ptrs_sptrs_by_segments
  // and add it to new_pos_list which is added to output_segments
  for (ColumnID column_id{0}; column_id < input_table->column_count(); ++column_id) {
    if (input_table->type() == TableType::References) {
      if (input_table->chunk_count() > 0) {
        const auto& input_table_pos_lists = input_pos_list_ptrs_sptrs_by_segments[column_id];

        auto iter = output_pos_list_cache.find(input_table_pos_lists);
        if (iter == output_pos_list_cache.end()) {
          // Get the row ids that are referenced
          auto new_pos_list = std::make_shared<RowIDPosList>(pos_list->size());
          auto new_pos_list_iter = new_pos_list->begin();
          auto common_chunk_id = std::optional<ChunkID>{};
          for (const auto& row : *pos_list) {
            if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
              *new_pos_list_iter = row;
              common_chunk_id = INVALID_CHUNK_ID;
            } else {
              const auto& referenced_pos_list = *(*input_table_pos_lists)[row.chunk_id];
              *new_pos_list_iter = referenced_pos_list[row.chunk_offset];

              // Check if the current row matches the ChunkIDs that we have seen in previous rows
              const auto referenced_chunk_id = referenced_pos_list[row.chunk_offset].chunk_id;
              if (!common_chunk_id) {
                common_chunk_id = referenced_chunk_id;
              } else if (*common_chunk_id != referenced_chunk_id) {
                common_chunk_id = INVALID_CHUNK_ID;
              }
            }
            ++new_pos_list_iter;
          }
          if (common_chunk_id && *common_chunk_id != INVALID_CHUNK_ID) {
            // Track the occuring chunk ids and set the single chunk guarantee if possible. Generally, this is the case
            // if both of the following are true: (1) The probe side input already had this guarantee and (2) no radix
            // partitioning was used. If multiple small PosLists were merged (see MIN_SIZE in join_hash.cpp), this
            // guarantee cannot be given.
            new_pos_list->guarantee_single_chunk();
          }

          iter = output_pos_list_cache.emplace(input_table_pos_lists, new_pos_list).first;
        }

        auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
            input_table->get_chunk(ChunkID{0})->get_segment(column_id));
        output_segments.push_back(std::make_shared<ReferenceSegment>(
            reference_segment->referenced_table(), reference_segment->referenced_column_id(), iter->second));
      } else {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
        // it.
        if (!dummy_table) dummy_table = Table::create_dummy_table(input_table->column_definitions());
        output_segments.push_back(std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list));
      }
    } else {
      // Check if the PosList references a single chunk. This is easier than tracking the flag through materialization,
      // radix partitioning, and so on. Also, actually checking for this property instead of simply forwarding it may
      // allows us to set guarantee_single_chunk in more cases. In cases where more than one chunk is referenced, this
      // should be cheap. In the other cases, the cost of iterating through the PosList are likely to be amortized in
      // following operators. See the comment at the previous call of guarantee_single_chunk to understand when this
      // guarantee might not be given.
      // This is not part of PosList as other operators should have a better understanding of how they emit references.
      auto common_chunk_id = std::optional<ChunkID>{};
      for (const auto& row : *pos_list) {
        if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
          common_chunk_id = INVALID_CHUNK_ID;
          break;
        } else {
          if (!common_chunk_id) {
            common_chunk_id = row.chunk_id;
          } else if (*common_chunk_id != row.chunk_id) {
            common_chunk_id = INVALID_CHUNK_ID;
            break;
          }
        }
      }
      if (common_chunk_id && *common_chunk_id != INVALID_CHUNK_ID) {
        pos_list->guarantee_single_chunk();
      }

      output_segments.push_back(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
    }
  }
}
}