#include "lqp_column_reference.hpp"

#include "boost/functional/hash.hpp"

#include "abstract_lqp_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

LQPColumnReference::LQPColumnReference(const std::shared_ptr<const AbstractLQPNode>& original_node,
                                       CxlumnID original_cxlumn_id)
    : _original_node(original_node), _original_cxlumn_id(original_cxlumn_id) {}

std::shared_ptr<const AbstractLQPNode> LQPColumnReference::original_node() const { return _original_node.lock(); }

CxlumnID LQPColumnReference::original_cxlumn_id() const { return _original_cxlumn_id; }

bool LQPColumnReference::operator==(const LQPColumnReference& rhs) const {
  return original_node() == rhs.original_node() && _original_cxlumn_id == rhs._original_cxlumn_id;
}

std::ostream& operator<<(std::ostream& os, const LQPColumnReference& column_reference) {
  const auto original_node = column_reference.original_node();
  Assert(original_node, "OriginalNode has expired");

  const auto stored_table_node = std::static_pointer_cast<const StoredTableNode>(column_reference.original_node());
  const auto table = StorageManager::get().get_table(stored_table_node->table_name);
  os << table->cxlumn_name(column_reference.original_cxlumn_id());

  return os;
}
}  // namespace opossum

namespace std {

size_t hash<opossum::LQPColumnReference>::operator()(const opossum::LQPColumnReference& column_reference) const {
  auto hash = boost::hash_value(column_reference.original_node().get());
  boost::hash_combine(hash, static_cast<size_t>(column_reference.original_cxlumn_id()));
  return hash;
}

}  // namespace std
