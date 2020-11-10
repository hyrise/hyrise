#include "validate.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "scheduler/job_task.hpp"
#include "storage/pos_lists/entire_chunk_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace {

bool is_row_visible(TransactionID our_tid, CommitID snapshot_commit_id, ChunkOffset chunk_offset,
                    const MvccData& mvcc_data) {
  const auto row_tid = mvcc_data.get_tid(chunk_offset);
  const auto begin_cid = mvcc_data.get_begin_cid(chunk_offset);
  const auto end_cid = mvcc_data.get_end_cid(chunk_offset);
  return Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid);
}

}  // namespace

bool Validate::is_row_visible(TransactionID our_tid, CommitID snapshot_commit_id, const TransactionID row_tid,
                              const CommitID begin_cid, const CommitID end_cid) {
  // Taken from: https://github.com/hyrise/hyrise-v1/blob/master/docs/documentation/queryexecution/tx.rst
  // auto own_insert = (our_tid == row_tid) && !(snapshot_commit_id >= begin_cid) && !(snapshot_commit_id >= end_cid);
  // auto past_insert = (our_tid != row_tid) && (snapshot_commit_id >= begin_cid) && !(snapshot_commit_id >= end_cid);
  // return own_insert || past_insert;

  // since gcc and clang are surprisingly bad at optimizing the above boolean expression, lets do that ourselves
  return snapshot_commit_id < end_cid && ((snapshot_commit_id >= begin_cid) != (row_tid == our_tid));
}

bool Validate::_is_entire_chunk_visible(const std::shared_ptr<const Chunk>& chunk,
                                        const CommitID snapshot_commit_id) const {
  Assert(
      _can_use_chunk_shortcut,
      "This call to _is_entire_chunk_visible is not allowed. Are there any DeleteOperators in the same transaction?");
  DebugAssert(!std::dynamic_pointer_cast<const ReferenceSegment>(chunk->get_segment(ColumnID{0})),
              "_is_entire_chunk_visible cannot be called on reference chunks.");

  const auto& mvcc_data = chunk->mvcc_data();
  const auto max_begin_cid = mvcc_data->max_begin_cid;
  if (!max_begin_cid) return false;

  return snapshot_commit_id >= max_begin_cid && chunk->invalid_row_count() == 0;
}

Validate::Validate(const std::shared_ptr<AbstractOperator>& in)
    : AbstractReadOnlyOperator(OperatorType::Validate, in) {}

const std::string& Validate::name() const {
  static const auto name = std::string{"Validate"};
  return name;
}

std::shared_ptr<AbstractOperator> Validate::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<Validate>(copied_left_input);
}

void Validate::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Validate::_on_execute() {
  Fail("Validate can't be called without a transaction context.");
}

std::shared_ptr<const Table> Validate::_on_execute(std::shared_ptr<TransactionContext> transaction_context) {
  DebugAssert(transaction_context, "Validate requires a valid TransactionContext.");
  DebugAssert(transaction_context->phase() == TransactionPhase::Active, "Transaction is not active anymore.");

  const auto in_table = left_input_table();
  const auto chunk_count = in_table->chunk_count();
  const auto our_tid = transaction_context->transaction_id();
  const auto snapshot_commit_id = transaction_context->snapshot_commit_id();

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(chunk_count);
  std::mutex output_mutex;

  auto job_start_chunk_id = ChunkID{0};
  auto job_end_chunk_id = ChunkID{0};
  auto job_row_count = uint32_t{0};

  // In some cases, we can identify a chunk as being entirely visible for the current transaction. Simply said,
  // if the youngest row in a chunk is visible, all other rows are older and hence visible, too. This applies if
  // (1) the chunk is immutable, i.e., no new rows can be added while this transaction is being executed,
  // (2) all rows in the chunk have been committed (i.e., their begin_cid has been set),
  // (3) the highest begin_cid in the chunk is lower than/equal to the snapshot_cid of the transaction
  //     (the max_begin_cid is stored in the chunk, not determined by the ValidateOperator),
  // (4) no rows in the chunk have been invalidated before this transaction was started,
  // (5) the current transaction has no in-flight deletes.
  const auto& read_write_operators = transaction_context->read_write_operators();
  for (const auto& read_write_operator : read_write_operators) {
    if (read_write_operator->type() == OperatorType::Delete) {
      _can_use_chunk_shortcut = false;
      break;
    }
  }

  while (job_end_chunk_id < chunk_count) {
    const auto chunk = in_table->get_chunk(job_end_chunk_id);
    Assert(chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    // Small chunks are bundled together to avoid unnecessary scheduling overhead.
    // Therefore, we count the number of rows to ensure a minimum of rows per job (default chunk size).
    job_row_count += chunk->size();
    if (job_row_count >= Chunk::DEFAULT_SIZE || job_end_chunk_id == (chunk_count - 1)) {
      // Single tasks are executed directly instead of scheduling a single job.
      bool execute_directly = job_start_chunk_id == 0 && job_end_chunk_id == (chunk_count - 1);

      if (execute_directly) {
        _validate_chunks(in_table, job_start_chunk_id, job_end_chunk_id, our_tid, snapshot_commit_id, output_chunks,
                         output_mutex);
      } else {
        jobs.push_back(std::make_shared<JobTask>([=, this, &output_chunks, &output_mutex] {
          _validate_chunks(in_table, job_start_chunk_id, job_end_chunk_id, our_tid, snapshot_commit_id, output_chunks,
                           output_mutex);
        }));

        // Prepare next job
        job_start_chunk_id = job_end_chunk_id + 1;
        job_row_count = 0;
      }
    }
    job_end_chunk_id++;
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  return std::make_shared<Table>(in_table->column_definitions(), TableType::References, std::move(output_chunks));
}

void Validate::_validate_chunks(const std::shared_ptr<const Table>& in_table, const ChunkID chunk_id_start,
                                const ChunkID chunk_id_end, const TransactionID our_tid,
                                const TransactionID snapshot_commit_id,
                                std::vector<std::shared_ptr<Chunk>>& output_chunks, std::mutex& output_mutex) const {
  // Stores whether a chunk has been found to be entirely visible. Only used for reference tables where no single
  // chunk guarantee has been given. Not stored in Validate object to avoid concurrency issues. This assumes that
  // only one table is referenced over all chunks. If, in the future, this is not true anymore, entirely_visible_chunks
  // either needs to be moved into the loop or turn into an `unordered_map<shared_ptr<Table>, vector<bool>>`.
  auto entirely_visible_chunks = std::vector<bool>{};
  auto entirely_visible_chunks_table = std::shared_ptr<const Table>{};  // used only for sanity check

  for (auto chunk_id = chunk_id_start; chunk_id <= chunk_id_end; ++chunk_id) {
    const auto chunk_in = in_table->get_chunk(chunk_id);
    Assert(chunk_in, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    const auto expected_number_of_valid_rows = chunk_in->size() - chunk_in->invalid_row_count();

    Segments output_segments;
    std::shared_ptr<const AbstractPosList> pos_list_out = std::make_shared<const RowIDPosList>();

    const auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(ColumnID{0}));

    // Holds the table that contains the MVCC information. For data segments, this is the table that we operate on.
    // If we are validating a reference segment, this is the table referenced by ref_segment_in.
    auto referenced_table = std::shared_ptr<const Table>{};

    // If the segments in this chunk reference a segment, build a poslist for a reference segment.
    if (ref_segment_in) {
      DebugAssert(chunk_in->references_exactly_one_table(),
                  "Input to Validate contains a Chunk referencing more than one table.");

      // Check all rows in the old poslist and put them in pos_list_out if they are visible.
      referenced_table = ref_segment_in->referenced_table();
      DebugAssert(referenced_table->uses_mvcc(), "Trying to use Validate on a table that has no MVCC data");

      if (!entirely_visible_chunks_table) {
        entirely_visible_chunks_table = referenced_table;
      } else {
        Assert(entirely_visible_chunks_table == referenced_table, "Input table references more than once table");
      }

      const auto& pos_list_in = ref_segment_in->pos_list();
      if (pos_list_in->references_single_chunk() && !pos_list_in->empty()) {
        // Fast path - we are looking at a single referenced chunk and thus need to get the MVCC data vector only once.
        const auto referenced_chunk = referenced_table->get_chunk(pos_list_in->common_chunk_id());
        auto mvcc_data = referenced_chunk->mvcc_data();

        if (_can_use_chunk_shortcut && _is_entire_chunk_visible(referenced_chunk, snapshot_commit_id)) {
          // We can reuse the old PosList since it is entirely visible. Not using the entirely_visible_chunks cache for
          // this shortcut to keep the code short.
          pos_list_out = pos_list_in;
        } else {
          RowIDPosList temp_pos_list;
          temp_pos_list.guarantee_single_chunk();
          for (auto row_id : *pos_list_in) {
            if (opossum::is_row_visible(our_tid, snapshot_commit_id, row_id.chunk_offset, *mvcc_data)) {
              temp_pos_list.emplace_back(row_id);
            }
          }
          pos_list_out = std::make_shared<const RowIDPosList>(std::move(temp_pos_list));
        }
      } else {
        // Slow path - we are looking at multiple referenced chunks and have to look at each row individually. We first
        // build a list of entirely visible chunks. Rows with chunk ids from that list do not need to be tested
        // individually. For chunk ids that are NOT in the list of entirely visible chunks, we need to actually look at
        // their MVCC information.
        RowIDPosList temp_pos_list;
        temp_pos_list.reserve(expected_number_of_valid_rows);

        if (entirely_visible_chunks.empty()) {
          // Check _is_entire_chunk_visible once for every chunk, even if we do not know if it is referenced or not.
          // While this might introduce a small overhead in the case of many unreferenced chunks, it allows us to avoid
          // a branch in the hot loop.
          entirely_visible_chunks = std::vector<bool>(referenced_table->chunk_count(), false);
          for (auto referenced_table_chunk_id = ChunkID{0}; referenced_table_chunk_id < referenced_table->chunk_count();
               ++referenced_table_chunk_id) {
            const auto referenced_chunk = referenced_table->get_chunk(referenced_table_chunk_id);
            entirely_visible_chunks[referenced_table_chunk_id] =
                _is_entire_chunk_visible(referenced_chunk, snapshot_commit_id);
          }
        }

        for (auto row_id : *pos_list_in) {
          if (entirely_visible_chunks[row_id.chunk_id]) {
            temp_pos_list.emplace_back(row_id);
            continue;
          }

          const auto referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);

          auto mvcc_data = referenced_chunk->mvcc_data();
          if (opossum::is_row_visible(our_tid, snapshot_commit_id, row_id.chunk_offset, *mvcc_data)) {
            temp_pos_list.emplace_back(row_id);
          }
        }
        pos_list_out = std::make_shared<const RowIDPosList>(std::move(temp_pos_list));
      }

      // Construct the actual ReferenceSegment objects and add them to the chunk.
      for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
        const auto reference_segment =
            std::static_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(column_id));
        const auto referenced_column_id = reference_segment->referenced_column_id();
        auto ref_segment_out = std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, pos_list_out);
        output_segments.push_back(ref_segment_out);
      }

      // Otherwise we have a non-reference Segment and simply iterate over all rows to build a poslist.
    } else {
      referenced_table = in_table;

      DebugAssert(chunk_in->has_mvcc_data(), "Trying to use Validate on a table that has no MVCC data");

      if (_can_use_chunk_shortcut && _is_entire_chunk_visible(chunk_in, snapshot_commit_id)) {
        // Not using the entirely_visible_chunks cache here as for data tables, we only look at chunks once anyway.
        pos_list_out = std::make_shared<EntireChunkPosList>(chunk_id, chunk_in->size());
      } else {
        const auto mvcc_data = chunk_in->mvcc_data();
        RowIDPosList temp_pos_list;
        temp_pos_list.reserve(expected_number_of_valid_rows);
        temp_pos_list.guarantee_single_chunk();
        // Generate pos_list_out.
        auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
        for (auto i = 0u; i < chunk_size; i++) {
          if (opossum::is_row_visible(our_tid, snapshot_commit_id, i, *mvcc_data)) {
            temp_pos_list.emplace_back(RowID{chunk_id, i});
          }
        }
        pos_list_out = std::make_shared<const RowIDPosList>(std::move(temp_pos_list));
      }

      // Create actual ReferenceSegment objects.
      for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
        auto ref_segment_out = std::make_shared<ReferenceSegment>(referenced_table, column_id, pos_list_out);
        output_segments.push_back(ref_segment_out);
      }
    }

    if (!pos_list_out->empty()) {
      std::lock_guard<std::mutex> lock(output_mutex);
      // The validate operator does not affect the sorted_by property. If a chunk has been sorted before, it still is
      // after the validate operator.
      const auto chunk = std::make_shared<Chunk>(output_segments);
      chunk->finalize();

      const auto& sorted_by = chunk_in->individually_sorted_by();
      if (!sorted_by.empty()) {
        chunk->set_individually_sorted_by(sorted_by);
      }
      output_chunks.emplace_back(chunk);
    }
  }
}

}  // namespace opossum
