#include "clustering_sorter.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

ClusteringSorter::ClusteringSorter(const std::shared_ptr<const AbstractOperator>& referencing_table_op, std::shared_ptr<Table> table, const std::set<ChunkID>& chunk_ids, const std::shared_ptr<const Table> sorted_table)
    : AbstractReadWriteOperator{OperatorType::Clustering, referencing_table_op}, _table{table}, _chunk_ids{chunk_ids}, _sorted_table{sorted_table} {
      size_t num_rows = 0;
      for (const auto chunk_id : _chunk_ids) {
        const auto& chunk = _table->get_chunk(chunk_id);
        Assert(chunk, "chunk disappeared");
        num_rows += chunk->size();
      }
      Assert(num_rows == _sorted_table->row_count(), "expected " + std::to_string(num_rows) + " rows but got " + std::to_string(_sorted_table->row_count()));
    }

const std::string& ClusteringSorter::name() const {
  static const auto name = std::string{"ClusteringSorter"};
  return name;
}

std::shared_ptr<const Table> ClusteringSorter::_on_execute(std::shared_ptr<TransactionContext> context) {
  // TODO get locks
  return nullptr;
}

void ClusteringSorter::_on_commit_records(const CommitID commit_id) {
  // all locks have been acquired by now

  // MVCC-delete the unsorted chunks
  for (const auto chunk_id : _chunk_ids) {
    const auto& chunk = _table->get_chunk(chunk_id);
    // TODO can this happen?
    Assert(chunk, "chunk disappeared");
    Assert(chunk->invalid_row_count() == 0, "chunk should not have invalid rows");

    const auto& mvcc_data = chunk->mvcc_data();
    for (ChunkOffset offset{0}; offset < chunk->size(); offset++) {
      mvcc_data->set_end_cid(offset, commit_id);
    }
    chunk->increase_invalid_row_count(chunk->size());
  }

  // copy the chunks from the sorted table over and update MVCC accordingly
  for (ChunkID chunk_id{0}; chunk_id < _sorted_table->chunk_count(); chunk_id++) {
    const auto& chunk = _sorted_table->get_chunk(chunk_id);
    Assert(chunk, "_sorted_table is not supposed to have removed chunks");

    Segments segments;
    for (ColumnID col_id{0}; col_id < chunk->column_count(); col_id++) {
      const auto& segment = chunk->get_segment(col_id);
      Assert(segment, "segment was null");
      segments.push_back(segment);
    }
    const auto mvcc_data = std::make_shared<MvccData>(chunk->size(), commit_id);

    // set ordering information
    Assert(chunk->ordered_by(), "chunk has no ordering information");
    const auto chunk_count = _table->chunk_count();    
    _table->append_chunk(segments, mvcc_data);
    Assert(_table->chunk_count() == chunk_count + 1, "some additional chunk was added");
    _table->get_chunk(chunk_count)->set_ordered_by(*chunk->ordered_by());
    _table->get_chunk(chunk_count)->finalize();
    ChunkEncoder::encode_chunks(_table, {chunk_count}, EncodingType::Dictionary);
  }
}

// TODO unlock on both commit and failure

void ClusteringSorter::_on_rollback_records() {
  
}

std::shared_ptr<AbstractOperator> ClusteringSorter::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  //return std::make_shared<Clustering>(copied_input_left);
  return nullptr;
}

void ClusteringSorter::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
