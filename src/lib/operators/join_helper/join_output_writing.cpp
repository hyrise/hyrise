#include "join_output_writing.hpp"

#include <unordered_map>

#include <boost/functional/hash_fwd.hpp>

#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterate.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

using PosLists = std::vector<std::shared_ptr<const AbstractPosList>>;
using PosListsByColumn = std::vector<std::shared_ptr<PosLists>>;   // NAME sucks due to https://github.com/hyrise/hyrise/pull/1076


struct PosListsHasher {
  size_t operator()(const PosLists& pos_lists) const {
    return boost::hash_range(pos_lists.begin(), pos_lists.end());
  }
};

/**
 * Returns a vector with the size of the input table's column count. Each value in this table 
 */
PosListsByColumn setup_pos_list_mapping(const std::shared_ptr<const Table>& input_table) {
  Assert(input_table->type() == TableType::References, "Function only works for reference tables");

  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();

  std::unordered_map<PosLists, std::shared_ptr<PosLists>, PosListsHasher> shared_pos_lists_by_pos_lists;
  shared_pos_lists_by_pos_lists.reserve(column_count);

  auto pos_lists_by_column = PosListsByColumn(column_count);
  auto pos_lists_by_column_it = pos_lists_by_column.begin();

  // For every column, for every chunk
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    // Get all the input pos lists so that we only have to pointer cast the segments once. Without storing the
    // pointers, we would need to fetch the position lists by fetching them from the input table (get chunk,
    // get segment, cast segment to obtain PosList) in write_output_segments.
    auto pos_list_ptrs = std::make_shared<PosLists>();
    pos_list_ptrs->reserve(chunk_count);

    // Iterate over every chunk and add the chunks segment with column_id to pos_list_ptrs
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

      const auto& ref_segment_uncasted = chunk->get_segment(column_id);
      const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(ref_segment_uncasted);
      pos_list_ptrs->push_back(ref_segment->pos_list());
    }

    DebugAssert(pos_list_ptrs->size() == chunk_count, "Fewer position lists than expected.");

    // pos_list_ptrs contains all position lists of the reference segments for the column_id.
    auto iter = shared_pos_lists_by_pos_lists.emplace(*pos_list_ptrs, pos_list_ptrs).first;

    *pos_lists_by_column_it = iter->second;
    ++pos_lists_by_column_it;
  }

  // std::cout << "column count is " << column_count << " and dist element coujnt is " << shared_pos_lists_by_pos_lists.size() << std::endl;

  // std::cout << "/Set up pos list " << std::endl;
  return pos_lists_by_column;
}

/**
 *
 * @param output_segments [in/out] Vector to which the newly created reference segments will be written.
 * @param input_table Table which all the position lists reference
 * @param input_pos_list_ptrs_sptrs_by_segments Contains all position lists of all columns of input table
 * @param pos_list contains the positions of rows to use from the input table
 */
void write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                                  const PosListsByColumn& input_pos_list_ptrs_sptrs_by_segments,
                                  std::shared_ptr<RowIDPosList>& pos_list) {
  std::unordered_map<std::shared_ptr<PosLists>, std::shared_ptr<RowIDPosList>> output_pos_list_cache;

  std::shared_ptr<Table> dummy_table;
  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();

  // Add segments from input table to output chunk.
  // For every column and every row in pos_list: get corresponding PosList of input_pos_list_ptrs_sptrs_by_segments
  // and add it to new_pos_list which is added to output_segments
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    if (input_table->type() == TableType::References) {
      if (chunk_count == 0) {
        // If there are no Chunks in the input_table, we can't deduce the Table that input_table is referencing to.
        // pos_list will contain only NULL_ROW_IDs anyway, so it doesn't matter which Table the ReferenceSegment that
        // we output is referencing. HACK, but works fine: we create a dummy table and let the ReferenceSegment ref
        // it.
        if (!dummy_table) {
          dummy_table = Table::create_dummy_table(input_table->column_definitions());
        }
        output_segments.push_back(std::make_shared<ReferenceSegment>(dummy_table, column_id, pos_list));
        continue;
      }

      // this is the list of Pos Lists for a column id
      const auto& input_table_pos_lists = input_pos_list_ptrs_sptrs_by_segments[column_id];

      Assert(output_pos_list_cache.size() > 0 || output_pos_list_cache.find(input_table_pos_lists) == output_pos_list_cache.end(), "Whoooot?");

      auto iter = output_pos_list_cache.find(input_table_pos_lists);
      if (iter == output_pos_list_cache.end()) {
        // Get the row ids that are referenced
        auto new_pos_list = std::make_shared<RowIDPosList>();
        new_pos_list->reserve(pos_list->size());

        auto common_chunk_id = std::optional<ChunkID>{};
        for (const auto& row : *pos_list) {
          if (row.chunk_offset == INVALID_CHUNK_OFFSET) {
            new_pos_list->push_back(row);
            common_chunk_id = INVALID_CHUNK_ID;
          } else {
            const auto& referenced_pos_list = *(*input_table_pos_lists)[row.chunk_id];
            new_pos_list->push_back(referenced_pos_list[row.chunk_offset]);

            // Check if the current row matches the ChunkIDs that we have seen in previous rows
            const auto referenced_chunk_id = referenced_pos_list[row.chunk_offset].chunk_id;
            if (!common_chunk_id) {
              common_chunk_id = referenced_chunk_id;
            } else if (*common_chunk_id != referenced_chunk_id) {
              common_chunk_id = INVALID_CHUNK_ID;
            }
          }
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
      // else {
      //   std::cout << "cache hit" << std::endl;
      // }

      auto reference_segment = std::static_pointer_cast<const ReferenceSegment>(
          input_table->get_chunk(ChunkID{0})->get_segment(column_id));
      output_segments.push_back(std::make_shared<ReferenceSegment>(
          reference_segment->referenced_table(), reference_segment->referenced_column_id(), iter->second));
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
}  // namespace

namespace hyrise {

std::vector<std::shared_ptr<Chunk>> write_output_chunks(
    std::vector<RowIDPosList>& pos_lists_left, std::vector<RowIDPosList>& pos_lists_right,
    const std::shared_ptr<const Table>& left_input_table, const std::shared_ptr<const Table>& right_input_table,
    bool create_left_side_pos_lists_by_column, bool create_right_side_pos_lists_by_column,
    OutputColumnOrder output_column_order, bool allow_partition_merge) {
  /**
     * Two Caches to avoid redundant reference materialization for Reference input tables. As there might be
     *  quite a lot Partitions (>500 seen), input Chunks (>500 seen), and columns (>50 seen), this speeds up
     *  write_output_chunks a lot.
     *
     * They do two things:
     *      - Make it possible to re-use output pos lists if two segments in the input table have exactly the same
     *          PosLists Chunk by Chunk
     *      - Avoid creating the std::vector<const RowIDPosList*> for each Partition over and over again.
     *
     * They hold one entry per column in the table, not per AbstractSegment in a single chunk
     */

  auto left_side_pos_lists_by_column = PosListsByColumn{};
  auto right_side_pos_lists_by_column = PosListsByColumn{};

  // Create tasks for post  
  auto create_left_side_pos_lists_by_column_job = [&]() {
    if (create_left_side_pos_lists_by_column) {
      left_side_pos_lists_by_column = setup_pos_list_mapping(left_input_table);
    }
  };
  auto create_right_side_pos_lists_by_column_job = [&]() {
    if (create_right_side_pos_lists_by_column) {
      right_side_pos_lists_by_column = setup_pos_list_mapping(right_input_table);
    }
  };

  auto pos_lists_by_column_tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  auto pos_lists_by_column_are_created = false;
  if ((create_left_side_pos_lists_by_column || create_left_side_pos_lists_by_column) &&
      (left_input_table->chunk_count() + right_input_table->chunk_count()) > 2048) {
    auto start = std::chrono::steady_clock::now();
    pos_lists_by_column_tasks.reserve(2);
    pos_lists_by_column_tasks.emplace_back(std::make_shared<JobTask>(create_left_side_pos_lists_by_column_job));
    pos_lists_by_column_tasks.emplace_back(std::make_shared<JobTask>(create_right_side_pos_lists_by_column_job));
    Hyrise::get().scheduler()->schedule_tasks(pos_lists_by_column_tasks);


    Hyrise::get().scheduler()->wait_for_tasks(pos_lists_by_column_tasks);
    auto end = std::chrono::steady_clock::now();
    std::cout << "Jobs:" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << " µs (" << left_input_table->chunk_count() << " & " << right_input_table->chunk_count() << ")" << std::endl;

    auto start2 = std::chrono::steady_clock::now();
    create_left_side_pos_lists_by_column_job();
    create_right_side_pos_lists_by_column_job();
    pos_lists_by_column_are_created = true;
    auto end2 = std::chrono::steady_clock::now();
    std::cout << "Funs:" << std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2).count() << " µs" << std::endl;
  } else {
    create_left_side_pos_lists_by_column_job();
    create_right_side_pos_lists_by_column_job();
    pos_lists_by_column_are_created = true;
  }

  const auto pos_lists_left_size = pos_lists_left.size();

  auto expected_output_chunk_count = size_t{0};
  for (auto partition_id = size_t{0}; partition_id < pos_lists_left_size; ++partition_id) {
    if (!pos_lists_left[partition_id].empty() || !pos_lists_right[partition_id].empty()) {
      ++expected_output_chunk_count;
    }
  }

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>(expected_output_chunk_count);
  auto write_output_segments_tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  // For every partition, create a reference segment.
  auto partition_id = size_t{0};
  auto chunk_input_position = size_t{0};

  while (partition_id < pos_lists_left_size) {
    // Moving the values into a shared pos list saves us some work in write_output_segments. We know that
    // left_side_pos_list and right_side_pos_list will not be used again.
    auto left_side_pos_list = std::make_shared<RowIDPosList>(std::move(pos_lists_left[partition_id]));
    auto right_side_pos_list = std::make_shared<RowIDPosList>(std::move(pos_lists_right[partition_id]));

    if (left_side_pos_list->empty() && right_side_pos_list->empty()) {
      ++partition_id;
      continue;
    }

    // If the input is heavily pre-filtered or the join results in very few matches, we might end up with a high
    // number of chunks that contain only few rows. If a PosList is smaller than MIN_SIZE, we merge it with the
    // following PosList(s) until a size between MIN_SIZE and MAX_SIZE is reached. This involves a trade-off:
    // A lower number of output chunks reduces the overhead, especially when multi-threading is used. However,
    // merging chunks destroys a potential references_single_chunk property of the PosList that would have been
    // emitted otherwise. Search for guarantee_single_chunk in join_hash_steps.hpp for details.
    constexpr auto MIN_SIZE = 500;
    constexpr auto MAX_SIZE = MIN_SIZE * 2;
    left_side_pos_list->reserve(MAX_SIZE);
    right_side_pos_list->reserve(MAX_SIZE);

    if (allow_partition_merge) {
      // Checking the probe side's PosLists is sufficient. The PosLists from the build side have either the same
      // size or are empty (in case of semi/anti joins).
      while (partition_id + 1 < pos_lists_right.size() && right_side_pos_list->size() < MIN_SIZE &&
             right_side_pos_list->size() + pos_lists_right[partition_id + 1].size() < MAX_SIZE) {
        // Copy entries from following PosList into the current working set (left_side_pos_list) and free the memory
        // used for the merged PosList.
        std::copy(pos_lists_left[partition_id + 1].begin(), pos_lists_left[partition_id + 1].end(),
                  std::back_inserter(*left_side_pos_list));
        pos_lists_left[partition_id + 1] = {};

        std::copy(pos_lists_right[partition_id + 1].begin(), pos_lists_right[partition_id + 1].end(),
                  std::back_inserter(*right_side_pos_list));
        pos_lists_right[partition_id + 1] = {};

        ++partition_id;
      }
    }

    if (!pos_lists_by_column_are_created) {
      Hyrise::get().scheduler()->wait_for_tasks(pos_lists_by_column_tasks);
      pos_lists_by_column_are_created = true;
    }

    // We need to pass the pos lists as parameters to ensure the shared_ptr is copied. Capturing by value result in
    // const shared_ptrs and write_output_segments expects non-const.
    auto write_output_segments_task = [&, chunk_input_position] (auto left_side_pos_list, auto right_side_pos_list) {
      Segments output_segments;
      // Swap back the inputs, so that the order of the output columns is not changed.
      switch (output_column_order) {
        case OutputColumnOrder::LeftFirstRightSecond:
          write_output_segments(output_segments, left_input_table, left_side_pos_lists_by_column, left_side_pos_list);
          write_output_segments(output_segments, right_input_table, right_side_pos_lists_by_column, right_side_pos_list);
          break;

        case OutputColumnOrder::RightFirstLeftSecond:
          write_output_segments(output_segments, right_input_table, right_side_pos_lists_by_column, right_side_pos_list);
          write_output_segments(output_segments, left_input_table, left_side_pos_lists_by_column, left_side_pos_list);
          break;

        case OutputColumnOrder::RightOnly:
          write_output_segments(output_segments, right_input_table, right_side_pos_lists_by_column, right_side_pos_list);
          break;
      }

      output_chunks[chunk_input_position] = std::make_shared<Chunk>(std::move(output_segments));
    };

    // Bind parameters to lambda before passing it to constructor of JobTask.
    auto write_output_segments_task_params = std::bind(write_output_segments_task, left_side_pos_list,
                                                       right_side_pos_list);
    write_output_segments_tasks.emplace_back(std::make_shared<JobTask>(write_output_segments_task_params));
    write_output_segments_tasks.back()->schedule();

    ++partition_id;
    ++chunk_input_position;
  }

  Hyrise::get().scheduler()->wait_for_tasks(write_output_segments_tasks);

  output_chunks.resize(chunk_input_position);
  return output_chunks;
}
}  // namespace hyrise
