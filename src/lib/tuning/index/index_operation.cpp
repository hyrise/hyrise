#include "index_operation.hpp"

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/logging.hpp"

namespace opossum {

void IndexOperation::execute() {
  if (create) {
    _create_index();
  } else {
    _delete_index();
  }
}

void IndexOperation::print_on(std::ostream& output) const {
  output << "IndexOperation{"
         << (create ? "Create" : "Delete")
         /*<< " " << type*/
         << " on " << column << "}";
}

void IndexOperation::_create_index() {
  auto table = StorageManager::get().get_table(column.table_name);
  auto chunk_count = table->chunk_count();

  std::vector<ColumnID> column_ids;
  column_ids.emplace_back(column.column_id);

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    switch (type) {
      case ColumnIndexType::GroupKey:
        chunk->create_index<GroupKeyIndex>(column_ids);
        break;
      case ColumnIndexType::CompositeGroupKey:
        chunk->create_index<CompositeGroupKeyIndex>(column_ids);
        break;
      case ColumnIndexType::AdaptiveRadixTree:
        chunk->create_index<AdaptiveRadixTreeIndex>(column_ids);
        break;
      default:
        LOG_WARN("Can not create invalid index type");
    }
  }
}

void IndexOperation::_delete_index() {
  auto table = StorageManager::get().get_table(column.table_name);
  auto chunk_count = table->chunk_count();

  std::vector<ColumnID> column_ids;
  column_ids.emplace_back(column.column_id);

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    // ToDo(group01): Currently there seems to be no way to remove an index from a chunk
  }
}

}  // namespace opossum
