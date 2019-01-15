#include "mvcc_delete_plugin.hpp"

#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "This is the Hyrise TestPlugin"; }

void MvccDeletePlugin::start() {
  for (const auto& table : sm.tables()) {
    const auto& chunks = table.second->chunks();

    for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunks.size(); chunk_id++) {
      const auto& chunk = chunks[chunk_id];

      if (chunk) {
        const double invalid_row_amount = static_cast<double>(chunk->invalid_row_count()) / chunk->size();
        if (invalid_row_amount >= DELETE_THRESHOLD) run_delete(table.first, chunk_id);
      }
    }
  }
}

void MvccDeletePlugin::stop() {
  //TODO: Implement if necessary
}

bool MvccDeletePlugin::run_delete(const std::string& table_name, ChunkID chunk_id) const {
  const auto table = sm.get_table(table_name);
  auto chunk = table->get_chunk(chunk_id);

  // ToDo: Maybe handle this as an edge case: -> Create a new chunk before Re-Insert
  DebugAssert(chunk_id < (table->chunk_count() - 1),
              "MVCC Logical Delete should not be applied on the last/current mutable chunk.");

  // Create temporary referencing table that contains the given chunk only
  auto table_filtered = _get_referencing_table(table_name, chunk_id);
  auto table_wrapper = std::make_shared<TableWrapper>(table_filtered);
  table_wrapper->execute();

  // Validate temporary table
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto validate_table = std::make_shared<Validate>(table_wrapper);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  // Use UPDATE operator to DELETE and RE-INSERT valid records in chunk
  // Pass validate_table as input twice since data will not be changed.
  auto update_table = std::make_shared<Update>(table_name, validate_table, validate_table);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();

  // Check for success
  if (update_table->execute_failed()) {
    return false;
  }

  // TODO(all): Check for success of commit, currently (2019-01-11) not possible.
  transaction_context->commit();

  // Mark chunk as logically deleted
  chunk->set_cleanup_commit_id(transaction_context->commit_id());
  return true;
}

std::shared_ptr<const Table> MvccDeletePlugin::_get_referencing_table(const std::string& table_name,
                                                                       const ChunkID chunk_id) const {
  const auto table_in = sm.get_table(table_name);
  const auto chunk_in = table_in->get_chunk(chunk_id);

  // Create new table
  auto table_out = std::make_shared<Table>(table_in->column_definitions(), TableType::References);

  DebugAssert(!std::dynamic_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(ColumnID{0})),
              "Only Value- or DictionarySegments can be used.");

  // Generate pos_list_out.
  auto pos_list_out = std::make_shared<PosList>();
  auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
  for (auto i = 0u; i < chunk_size; i++) {
    pos_list_out->emplace_back(RowID{chunk_id, i});
  }

  // Create actual ReferenceSegment objects.
  Segments output_segments;
  for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
    auto ref_segment_out = std::make_shared<ReferenceSegment>(table_in, column_id, pos_list_out);
    output_segments.push_back(ref_segment_out);
  }

  if (!pos_list_out->empty() > 0) {
    table_out->append_chunk(output_segments);
  }

  return table_out;
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum
