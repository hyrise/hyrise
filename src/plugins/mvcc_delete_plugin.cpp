#include "mvcc_delete_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "This is the Hyrise TestPlugin"; }

void MvccDeletePlugin::start() {

  // TODO(anyone) Put this into a separate function, e.g. _analyze_chunks()
  for (const auto& table : sm.tables()) {
    const auto& chunks = table.second->chunks();

    for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunks.size(); chunk_id++) {
      const auto& chunk = chunks[chunk_id];

      if (chunk) {
        const double invalid_row_amount = static_cast<double>(chunk->invalid_row_count()) / chunk->size();
        if (invalid_row_amount == 1) {
          // TODO(anyone) Implement update if e.g. 0.9 < invalid_row_amaount < 1

          table.second->delete_chunk(chunk_id);
        }
      }
    }
  }
}

void MvccDeletePlugin::stop() {
  //TODO: Implement if necessary
}


void MvccDeletePlugin::_clean_up_chunk(const std::string &table_name, opossum::ChunkID chunk_id) {

  // Delete chunk logically
  bool success = MvccDelete::delete_chunk_logically(table_name, chunk_id);

  // Queue physical delete
  if (success) {
    DebugAssert(StorageManager::get().get_table(table_name)->get_chunk(chunk_id)->get_cleanup_commit_id()
    != MvccData::MAX_COMMIT_ID, "Chunk needs to be deleted logically before deleting it physically.")
    _physical_delete_queue.emplace(table_name, chunk_id);
  }
}

void MvccDeletePlugin::_process_physical_delete_queue() {

  bool success = true;
  while(!_physical_delete_queue.empty() && success) {

    ChunkSpecifier chunk_spec = _physical_delete_queue.front();
    success = MvccDelete::delete_chunk_physically(chunk_spec.table_name, chunk_spec.chunk_id);

    if(success) {
      _physical_delete_queue.pop();
    }
  }

}


EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum
