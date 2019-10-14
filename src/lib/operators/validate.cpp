#include "validate.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "operators/delete.hpp"
#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

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

bool Validate::_is_entire_chunk_visible(const std::shared_ptr<const Chunk>& chunk, CommitID snapshot_commit_id, const SharedScopedLockingPtr<MvccData>& mvcc_data) {
  const auto max_begin_cid = mvcc_data->max_begin_cid;
  if (!max_begin_cid) return false;

  return snapshot_commit_id >= max_begin_cid && chunk->invalid_row_count() == 0;
}

std::shared_ptr<const Table> Validate::_on_execute(std::shared_ptr<TransactionContext> transaction_context) {
  DebugAssert(transaction_context, "Validate requires a valid TransactionContext.");
  DebugAssert(transaction_context->phase() == TransactionPhase::Active, "Transaction is not active anymore.");

  const auto in_table = input_table_left();
  const auto chunk_count = in_table->chunk_count();
  const auto our_tid = transaction_context->transaction_id();
  const auto snapshot_commit_id = transaction_context->snapshot_commit_id();

  // Optimization, only checking chunks, blabla
  // if (in_table->type() == TableType::Data) {
  //   bool check_chunks_for_visibility = true;

  //   // Abort if there is a delete in the same transaction context
  //   const auto& rw_operators = transaction_context->read_write_operators();
  //   for (const auto& rw_operator : rw_operators) {
  //     if (rw_operator->type() == OperatorType::Delete) {
  //       check_chunks_for_visibility = false;
  //       break;
  //     }
  //   }

  //   if (check_chunks_for_visibility) {
  //     ChunkID visible_chunks = ChunkID{0};
  //     for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
  //       const auto chunk_in = in_table->get_chunk(chunk_id);
  //       DebugAssert(chunk_in->has_mvcc_data(), "Trying to use Validate on a table that has no MVCC data");

  //       if (!_is_entire_chunk_visible(chunk_in, snapshot_commit_id)) break;

  //       ++visible_chunks;
  //     }

  //     if (visible_chunks == chunk_count) return in_table;
  //   }
  // }

  // for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
  std::vector<std::shared_ptr<JobTask>> jobs;
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(chunk_count);
  std::mutex output_mutex;

  auto job_start_chunk_id = ChunkID{0};
  auto job_end_chunk_id = ChunkID{0};
  auto job_row_count = uint32_t{0};

  bool check_visibility_on_chunk_level = true;
  const auto& rw_operators = transaction_context->read_write_operators();

  // Not allowed if any modifiying operator is registerd in the same transaction context
  // if (rw_operators.empty())
  //   check_visibility_on_chunk_level = true;

  // Not allowed if a delete is registerd in the same transaction context
  for (const auto& rw_operator : rw_operators) {
    if (rw_operator->type() == OperatorType::Delete) {
      check_visibility_on_chunk_level = false;
      break;
    }
  }

  while (job_end_chunk_id < chunk_count) {
    const auto chunk = in_table->get_chunk(job_end_chunk_id);
    Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

    // Small chunks are bundled together to avoid unnecessary scheduling overhead.
    // Therefore, we count the number of rows to ensure a minimum of rows per job (default chunk size).
    job_row_count += chunk->size();
    if (job_row_count >= Chunk::DEFAULT_SIZE || job_end_chunk_id == (chunk_count - 1)) {
      // Single tasks are executed directly instead of scheduling a single job.
      bool execute_directly = job_start_chunk_id == 0 && job_end_chunk_id == (chunk_count - 1);

      if (execute_directly) {
        _validate_chunks(in_table, job_start_chunk_id, job_end_chunk_id, our_tid, snapshot_commit_id, output_chunks,
                         output_mutex, check_visibility_on_chunk_level);
      } else {
        jobs.push_back(std::make_shared<JobTask>([=, this, &output_chunks, &output_mutex] {
          _validate_chunks(in_table, job_start_chunk_id, job_end_chunk_id, our_tid, snapshot_commit_id, output_chunks,
                           output_mutex, check_visibility_on_chunk_level);
        }));
        jobs.back()->schedule();

        // Prepare next job
        job_start_chunk_id = job_end_chunk_id + 1;
        job_row_count = 0;
      }
    }
    job_end_chunk_id++;
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);

  return std::make_shared<Table>(in_table->column_definitions(), TableType::References, std::move(output_chunks));
}

void Validate::_validate_chunks(const std::shared_ptr<const Table>& in_table, const ChunkID chunk_id_start,
                                const ChunkID chunk_id_end, const TransactionID our_tid,
                                const TransactionID snapshot_commit_id,
                                std::vector<std::shared_ptr<Chunk>>& output_chunks, std::mutex& output_mutex,
                                const bool check_visibility_on_chunk_level) {
  for (auto chunk_id = chunk_id_start; chunk_id <= chunk_id_end; ++chunk_id) {
    const auto chunk_in = in_table->get_chunk(chunk_id);
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

        // if (!mvcc_data->dirty && _is_entire_chunk_visible(our_tid, snapshot_commit_id, *mvcc_data)) {
        //   // *pos_list_out = pos_list_in.copy();
        //   pos_list_out->resize(pos_list_in.size());
        //   std::memcpy(pos_list_out->data(), pos_list_in.data(), pos_list_in.size() * sizeof(RowID));
        //   // auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
        //   // pos_list_out->resize(chunk_size);
        //   // for (auto i = 0u; i < chunk_size; i++) {
        //   //   (*pos_list_out)[i] = RowID{chunk_id, i};
        //   // }
        // } else {

        if (check_visibility_on_chunk_level && _is_entire_chunk_visible(chunk_in, snapshot_commit_id, mvcc_data)) {
          std::cout << "Case 1.1.1" << std::endl;
          const auto pos_list_size = pos_list_in.size();
          pos_list_out->resize(pos_list_size);
          // for (size_t row = 0; row < pos_list_size; ++row) {
            // (*pos_list_out)[i] = RowID{chunk_id, i};
          // }
          std::memcpy(pos_list_out->data(), pos_list_in.data(), pos_list_in.size() * sizeof(RowID));
        } else {
          std::cout << "Case 1.1.2" << std::endl;
          for (auto row_id : pos_list_in) {
            if (opossum::is_row_visible(our_tid, snapshot_commit_id, row_id.chunk_offset, *mvcc_data)) {
              pos_list_out->emplace_back(row_id);
            }
          }
        }

      } else {
        // Slow path - we are looking at multiple referenced chunks and need to get the MVCC data vector for every row.
        std::cout << "Case 1.2.1" << std::endl;
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

      // Otherwise we have a non-reference Segment and simply iterate over all rows to build a poslist.
    } else {
      referenced_table = in_table;
      DebugAssert(chunk_in->has_mvcc_data(), "Trying to use Validate on a table that has no MVCC data");
      const auto mvcc_data = chunk_in->get_scoped_mvcc_data_lock();
      pos_list_out->guarantee_single_chunk();

      // if (!mvcc_data->dirty && _is_entire_chunk_visible(our_tid, snapshot_commit_id, *mvcc_data)) {
      //   // return;
      //   auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
      //   pos_list_out->resize(chunk_size);
      //   for (auto i = 0u; i < chunk_size; i++) {
      //     (*pos_list_out)[i] = RowID{chunk_id, i};
      //   }
      // } else {

      if (check_visibility_on_chunk_level && _is_entire_chunk_visible(chunk_in, snapshot_commit_id, mvcc_data)) {
        std::cout << "Case 2.1.1" << std::endl;
        const auto chunk_size = chunk_in->size();
        pos_list_out->resize(chunk_size);
        for (auto chunk_offset = 0u; chunk_offset < chunk_size; ++chunk_offset) {
          (*pos_list_out)[chunk_offset] = RowID{chunk_id, chunk_offset};
        }
      } else {
        // Generate pos_list_out.
        std::cout << "Case 2.1.2" << std::endl;
        auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
        for (auto i = 0u; i < chunk_size; i++) {
          if (opossum::is_row_visible(our_tid, snapshot_commit_id, i, *mvcc_data)) {
            pos_list_out->emplace_back(RowID{chunk_id, i});
          }
        }
      }

      // Create actual ReferenceSegment objects.
      for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
        auto ref_segment_out = std::make_shared<ReferenceSegment>(referenced_table, column_id, pos_list_out);
        output_segments.push_back(ref_segment_out);
      }
    }

    if (!pos_list_out->empty() > 0) {
      std::lock_guard<std::mutex> lock(output_mutex);
      output_chunks.emplace_back(std::make_shared<Chunk>(output_segments));
    }
  }
}

}  // namespace opossum
