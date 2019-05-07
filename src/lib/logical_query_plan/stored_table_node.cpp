#include "stored_table_node.hpp"

#include "expression/lqp_column_expression.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::StoredTable), table_name(table_name) {}

LQPColumnReference StoredTableNode::get_column(const std::string& name) const {
  const auto table = StorageManager::get().get_table(table_name);
  const auto column_id = table->column_id_by_name(name);
  return {shared_from_this(), column_id};
}

void StoredTableNode::set_pruned_chunk_ids(const std::vector<ChunkID>& pruned_chunk_ids) {
  DebugAssert(std::is_sorted(pruned_chunk_ids.begin(), pruned_chunk_ids.end()), "Expected sorted vector of ChunkIDs");

  _pruned_chunk_ids = pruned_chunk_ids;
}

const std::vector<ChunkID>& StoredTableNode::pruned_chunk_ids() const { return _pruned_chunk_ids; }

void StoredTableNode::set_pruned_column_ids(const std::vector<ColumnID>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");

  // We cannot create a Table without columns - since Chunks rely on their first column to determine their row count
  const auto stored_column_count = StorageManager::get().get_table(table_name)->column_count();
  Assert(pruned_column_ids.size() < stored_column_count, "Cannot exclude all columns from Table.");

  _pruned_column_ids = pruned_column_ids;

  // Rebuilding this lazily the next time `column_expressions()` is called
  _column_expressions.reset();
}

const std::vector<ColumnID>& StoredTableNode::pruned_column_ids() const { return _pruned_column_ids; }

std::string StoredTableNode::description() const {
  std::ostringstream stream;
  stream << "[StoredTable] Name: '" << table_name << "'";

  if (!_pruned_chunk_ids.empty()) {
    stream << " pruned chunks: " << _pruned_chunk_ids.size();
  }

  if (!_pruned_column_ids.empty()) {
    stream << " pruned columns: " << _pruned_column_ids.size() << "/"
           << StorageManager::get().get_table(table_name)->column_count();
  }

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& StoredTableNode::column_expressions() const {
  // Need to initialize the expressions lazily because a) they will have a weak_ptr to this node and we can't obtain
  // that in the constructor and b) because we don't have column pruning information in the constructor
  if (!_column_expressions) {
    const auto table = StorageManager::get().get_table(table_name);

    // Build `_expression` with respect to the `_pruned_column_ids`
    _column_expressions.emplace(table->column_count() - _pruned_column_ids.size());

    auto pruned_column_ids_iter = _pruned_column_ids.begin();
    for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0}; stored_column_id < table->column_count();
         ++stored_column_id) {
      if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
        ++pruned_column_ids_iter;
        continue;
      }

      (*_column_expressions)[output_column_id] =
          std::make_shared<LQPColumnExpression>(LQPColumnReference{shared_from_this(), stored_column_id});
      ++output_column_id;
    }
  }

  return *_column_expressions;
}

bool StoredTableNode::is_column_nullable(const ColumnID column_id) const {
  const auto table = StorageManager::get().get_table(table_name);
  return table->column_is_nullable(column_id);
}

std::shared_ptr<TableStatistics> StoredTableNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(!left_input && !right_input, "StoredTableNode must be leaf");

  const auto stored_statistics = StorageManager::get().get_table(table_name)->table_statistics();

  if (_pruned_column_ids.empty()) {
    return stored_statistics;
  }

  /**
   * Prune `_pruned_column_ids` from the statistics
   */

  auto output_column_statistics = std::vector<std::shared_ptr<const BaseColumnStatistics>>{
      stored_statistics->column_statistics().size() - _pruned_column_ids.size()};

  auto pruned_column_ids_iter = _pruned_column_ids.begin();

  for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0};
       stored_column_id < stored_statistics->column_statistics().size(); ++stored_column_id) {
    if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
      ++pruned_column_ids_iter;
      continue;
    }

    output_column_statistics[output_column_id] = stored_statistics->column_statistics()[stored_column_id];
    ++output_column_id;
  }

  return std::make_shared<TableStatistics>(stored_statistics->table_type(), stored_statistics->row_count(),
                                           output_column_statistics);
}

std::shared_ptr<AbstractLQPNode> StoredTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto copy = make(table_name);
  copy->set_pruned_chunk_ids(_pruned_chunk_ids);
  copy->set_pruned_column_ids(_pruned_column_ids);
  return copy;
}

bool StoredTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& stored_table_node = static_cast<const StoredTableNode&>(rhs);
  return table_name == stored_table_node.table_name && _pruned_chunk_ids == stored_table_node._pruned_chunk_ids &&
         _pruned_column_ids == stored_table_node._pruned_column_ids;
}

}  // namespace opossum
