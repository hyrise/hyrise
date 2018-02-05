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
  switch (type) {
    case ColumnIndexType::GroupKey:
      table->create_index<GroupKeyIndex>(column.column_ids);
      break;
    case ColumnIndexType::CompositeGroupKey:
      table->create_index<CompositeGroupKeyIndex>(column.column_ids);
      break;
    case ColumnIndexType::AdaptiveRadixTree:
      table->create_index<AdaptiveRadixTreeIndex>(column.column_ids);
      break;
    default:
      LOG_WARN("Can not create invalid index type");
  }
}

void IndexOperation::_delete_index() {
  auto table = StorageManager::get().get_table(column.table_name);
  auto chunk_count = table->chunk_count();
  // ToDo(group01): index removal on chunks is inconsistent with index creation on tables...

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    auto index = chunk->get_index(type, column.column_ids);
    if (!index) {
        LOG_WARN("Couldn't find specified index for deletion");
        continue;
    }
    chunk->remove_index(index);
  }
}

}  // namespace opossum
