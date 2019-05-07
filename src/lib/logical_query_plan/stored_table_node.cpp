#include "stored_table_node.hpp"

#include "expression/lqp_column_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::StoredTable), table_name(table_name) {}

LQPColumnReference StoredTableNode::get_column(const std::string& name) const {
  const auto table = StorageManager::get().get_table(table_name);
  const auto column_id = table->column_id_by_name(name);
  return {shared_from_this(), column_id};
}

void StoredTableNode::set_excluded_chunk_ids(const std::vector<ChunkID>& chunks) { _excluded_chunk_ids = chunks; }

const std::vector<ChunkID>& StoredTableNode::excluded_chunk_ids() const { return _excluded_chunk_ids; }

void StoredTableNode::set_excluded_column_ids(const std::vector<ColumnID>& column_ids) {
  Assert(column_ids.size() < StorageManager::get().get_table(table_name)->column_count(), "Cannot exclude all columns from Table.");

  _excluded_column_ids = column_ids;
  std::sort(_excluded_column_ids.begin(), _excluded_column_ids.end());
  const auto last = std::unique(_excluded_column_ids.begin(), _excluded_column_ids.end());
  _excluded_column_ids.erase(last,  _excluded_column_ids.end());
  _expressions.reset();
}

const std::vector<ColumnID>& StoredTableNode::excluded_column_ids() const {
  return _excluded_column_ids;
}

std::string StoredTableNode::description() const {

  std::ostringstream stream;
  stream << "[StoredTable] Name: '" << table_name << "'";

  if (!_excluded_column_ids.empty()) {
    stream << " Excluded columns: " << _excluded_column_ids.size() << "/" << StorageManager::get().get_table(table_name)->column_count();
  }

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& StoredTableNode::column_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain that
  // in the constructor
  if (!_expressions) {
    const auto table = StorageManager::get().get_table(table_name);

    _expressions.emplace(table->column_count() - _excluded_column_ids.size());

    auto excluded_column_ids_iter = _excluded_column_ids.begin();

    for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0}; stored_column_id < table->column_count(); ++stored_column_id) {
      if (excluded_column_ids_iter != _excluded_column_ids.end() && stored_column_id == *excluded_column_ids_iter) {
        ++excluded_column_ids_iter;
        continue;
      }

      (*_expressions)[output_column_id] =
          std::make_shared<LQPColumnExpression>(LQPColumnReference{shared_from_this(), stored_column_id});
      ++output_column_id;
    }
  }

  return *_expressions;
}

bool StoredTableNode::is_column_nullable(const ColumnID column_id) const {
  const auto table = StorageManager::get().get_table(table_name);
  return table->column_is_nullable(column_id);
}

std::shared_ptr<TableStatistics> StoredTableNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(!left_input && !right_input, "StoredTableNode must be leaf");

  const auto original_statistics = StorageManager::get().get_table(table_name)->table_statistics();

  if (_excluded_column_ids.empty()) {
    return original_statistics;
  }

  // Prune _excluded_column_ids from the statistics
  auto column_statistics = std::vector<std::shared_ptr<const BaseColumnStatistics>>{original_statistics->column_statistics().size() - _excluded_column_ids.size()};

  auto excluded_column_ids_iter = _excluded_column_ids.begin();

  for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0}; stored_column_id < original_statistics->column_statistics().size(); ++stored_column_id) {
    if (excluded_column_ids_iter != _excluded_column_ids.end() && stored_column_id == *excluded_column_ids_iter) {
      ++excluded_column_ids_iter;
      continue;
    }

    column_statistics[output_column_id] = original_statistics->column_statistics()[stored_column_id];
    ++output_column_id;
  }

  return std::make_shared<TableStatistics>(original_statistics->table_type(), original_statistics->row_count(), column_statistics);
}

std::shared_ptr<AbstractLQPNode> StoredTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto copy = make(table_name);
  copy->set_excluded_chunk_ids(_excluded_chunk_ids);
  return copy;
}

bool StoredTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& stored_table_node = static_cast<const StoredTableNode&>(rhs);
  return table_name == stored_table_node.table_name && _excluded_chunk_ids == stored_table_node._excluded_chunk_ids && _excluded_column_ids == stored_table_node._excluded_column_ids;
}

}  // namespace opossum
