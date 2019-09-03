#include "validate.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/current_scheduler.hpp"

namespace opossum {

namespace {

bool is_row_visible(TransactionID our_tid, CommitID snapshot_commit_id, ChunkOffset chunk_offset,
                    const MvccData& mvcc_data) {
  const auto row_tid = mvcc_data.tids[chunk_offset].load();
  const auto begin_cid = mvcc_data.begin_cids[chunk_offset];
  const auto end_cid = mvcc_data.end_cids[chunk_offset];
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

Validate::Validate(const std::shared_ptr<AbstractOperator>& in)
    : AbstractReadOnlyOperator(OperatorType::Validate, in) {}

const std::string Validate::name() const { return "Validate"; }

std::shared_ptr<AbstractOperator> Validate::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Validate>(copied_input_left);
}

void Validate::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Validate::_on_execute() {
  Fail("Validate can't be called without a transaction context.");
}

std::shared_ptr<const Table> Validate::_on_execute(std::shared_ptr<TransactionContext> transaction_context) {
  DebugAssert(transaction_context, "Validate requires a valid TransactionContext.");
  DebugAssert(transaction_context->phase() == TransactionPhase::Active, "Transaction is not active anymore.");

  const auto chunk_count = input_table_left()->chunk_count();

  const auto our_tid = transaction_context->transaction_id();
  const auto snapshot_commit_id = transaction_context->snapshot_commit_id();

  std::vector<std::shared_ptr<JobTask>> jobs;
  std::vector<std::shared_ptr<std::vector<std::shared_ptr<Chunk>>>> job_results;
  auto job_chunk_id_start = ChunkID{0};
  auto job_row_count = uint32_t{0};
  for (auto chunk_id = job_chunk_id_start; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table_left()->get_chunk(chunk_id);
    job_row_count += chunk->size();

    if(job_row_count >= Chunk::DEFAULT_SIZE || chunk_id == (chunk_count - 1)) {
      bool execute_directly = job_chunk_id_start == 0 && chunk_id == (chunk_count - 1);

      auto output_chunks = std::make_shared<std::vector<std::shared_ptr<Chunk>>>();
      output_chunks->reserve(chunk_id - job_chunk_id_start);

      if(execute_directly) {
        _process_chunks(job_chunk_id_start, chunk_id, our_tid, snapshot_commit_id, output_chunks);
      } else {
        jobs.push_back(std::make_shared<JobTask>([=, this] {
          _process_chunks(job_chunk_id_start, chunk_id, our_tid, snapshot_commit_id, output_chunks);
        }));
        jobs.back()->schedule();

        job_chunk_id_start = chunk_id + 1;
        job_row_count = uint32_t{0};
      }

      job_results.push_back(output_chunks);
    }
  }

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};
  output_chunks.reserve(chunk_count);

  if(jobs.size() > 0) {
    // Merge job results
    CurrentScheduler::wait_for_tasks(jobs);
    for (const auto& chunk_vector : job_results) {
      for (const auto& chunk : *chunk_vector) {
        output_chunks.emplace_back(chunk);
      }
    }
  } else if(job_results.size() > 0) {
    Assert(job_results.size() == 1, "Direct execution should not produce multiple result vectors.");
    output_chunks = *job_results.front();
  }

  return std::make_shared<Table>(input_table_left()->column_definitions(), TableType::References, std::move(output_chunks));
}

void Validate::_process_chunks(const ChunkID chunk_id_start, const ChunkID chunk_id_end, const TransactionID our_tid, const TransactionID snapshot_commit_id, std::shared_ptr<std::vector<std::shared_ptr<Chunk>>> output_chunks) {

  for (auto chunk_id = chunk_id_start; chunk_id <= chunk_id_end; ++chunk_id) {
    const auto chunk_in = input_table_left()->get_chunk(chunk_id);
    Assert(chunk_in, "Did not expect deleted chunk here.");  // see #1686

    Segments output_segments;
    auto pos_list_out = std::make_shared<PosList>();
    auto referenced_table = std::shared_ptr<const Table>();
    const auto ref_segment_in = std::dynamic_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(ColumnID{0}));

    // If the segments in this chunk reference a segment, build a poslist for a reference segment.
    if (ref_segment_in) {
      DebugAssert(chunk_in->references_exactly_one_table(),
                  "Input to Validate contains a Chunk referencing more than one table.");

      // Check all rows in the old poslist and put them in pos_list_out if they are visible.
      referenced_table = ref_segment_in->referenced_table();
      DebugAssert(referenced_table->has_mvcc(), "Trying to use Validate on a table that has no MVCC data");

      const auto& pos_list_in = *ref_segment_in->pos_list();
      if (pos_list_in.references_single_chunk() && !pos_list_in.empty()) {
        // Fast path - we are looking at a single referenced chunk and thus need to get the MVCC data vector only once.

        pos_list_out->guarantee_single_chunk();

        const auto referenced_chunk = referenced_table->get_chunk(pos_list_in.common_chunk_id());
        auto mvcc_data = referenced_chunk->get_scoped_mvcc_data_lock();

        for (auto row_id : pos_list_in) {
          if (opossum::is_row_visible(our_tid, snapshot_commit_id, row_id.chunk_offset, *mvcc_data)) {
            pos_list_out->emplace_back(row_id);
          }
        }

      } else {
        // Slow path - we are looking at multiple referenced chunks and need to get the MVCC data vector for every row.

        for (auto row_id : pos_list_in) {
          const auto referenced_chunk = referenced_table->get_chunk(row_id.chunk_id);

          auto mvcc_data = referenced_chunk->get_scoped_mvcc_data_lock();

          if (opossum::is_row_visible(our_tid, snapshot_commit_id, row_id.chunk_offset, *mvcc_data)) {
            pos_list_out->emplace_back(row_id);
          }
        }
      }

      // Construct the actual ReferenceSegment objects and add them to the chunk.
      for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
        const auto reference_segment =
            std::static_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(column_id));
        const auto referenced_column_id = reference_segment->referenced_column_id();
        auto ref_segment_out = std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, pos_list_out);
        output_segments.push_back(ref_segment_out);
      }

      // Otherwise we have a Value- or DictionarySegment and simply iterate over all rows to build a poslist.
    } else {
      referenced_table = input_table_left();
      DebugAssert(chunk_in->has_mvcc_data(), "Trying to use Validate on a table that has no MVCC data");
      const auto mvcc_data = chunk_in->get_scoped_mvcc_data_lock();
      pos_list_out->guarantee_single_chunk();

      // Generate pos_list_out.
      auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
      for (auto i = 0u; i < chunk_size; i++) {
        if (opossum::is_row_visible(our_tid, snapshot_commit_id, i, *mvcc_data)) {
          pos_list_out->emplace_back(RowID{chunk_id, i});
        }
      }

      // Create actual ReferenceSegment objects.
      for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
        auto ref_segment_out = std::make_shared<ReferenceSegment>(referenced_table, column_id, pos_list_out);
        output_segments.push_back(ref_segment_out);
      }
    }

    if (!pos_list_out->empty() > 0) {
      output_chunks->emplace_back(std::make_shared<Chunk>(output_segments));
    }
  }
}

}  // namespace opossum
