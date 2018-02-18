#include "index_operation.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/logging.hpp"

namespace opossum {

void IndexOperation::execute() {
  if (_create) {
    _create_index();
  } else {
    _delete_index();
  }
}

void IndexOperation::print_on(std::ostream& output) const {
  output << "IndexOperation{" << (_create ? "Create" : "Delete") << " on " << _column << "}";
}

void IndexOperation::_create_index() {
  auto table = StorageManager::get().get_table(_column.table_name);
  switch (_type) {
    case ColumnIndexType::GroupKey:
      table->create_index<GroupKeyIndex>(_column.column_ids);
      break;
    case ColumnIndexType::CompositeGroupKey:
      table->create_index<CompositeGroupKeyIndex>(_column.column_ids);
      break;
    case ColumnIndexType::AdaptiveRadixTree:
      table->create_index<AdaptiveRadixTreeIndex>(_column.column_ids);
      break;
    default:
      Fail("Can not create invalid/unknown index type");
  }

  _invalidate_cache();
}

void IndexOperation::_delete_index() {
  auto table = StorageManager::get().get_table(_column.table_name);

  IndexInfo chosen_index_info{std::vector<ColumnID>{}, "", ColumnIndexType::Invalid};
  for (auto index_info : table->get_indexes()) {
    // The index name is ignored in comparison, as it seems not to be used anywhere
    if (index_info.type == _type && index_info.column_ids == _column.column_ids) {
      chosen_index_info = index_info;
      break;
    }
  }

  if (chosen_index_info.type == ColumnIndexType::Invalid) {
    Fail("Index to be deleted was not found");
  }

  table->remove_index(chosen_index_info);

  _invalidate_cache();
}

void IndexOperation::_invalidate_cache() {
  /*
     * For now, this simply clears the cache.
     * However, it would be more beneficial to *selectively* evict cache entries
     * that are directly affected by this change.
     * Maybe it makes sense to even re-create those entries with an updated
     * query plan...
     */
  SQLQueryCache<std::shared_ptr<AbstractLQPNode>>::get().clear();
  SQLQueryCache<SQLQueryPlan>::get().clear();
}

const ColumnRef& IndexOperation::column() const { return _column; }

ColumnIndexType IndexOperation::type() { return _type; }

bool IndexOperation::create() { return _create; }

}  // namespace opossum
