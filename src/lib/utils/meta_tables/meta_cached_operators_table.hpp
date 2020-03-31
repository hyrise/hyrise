#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

#include "operators/abstract_operator.hpp"

namespace opossum {

/**
 * This is a class for showing all cached PQP operators via a meta table.
 */
class MetaCachedOperatorsTable : public AbstractMetaTable {
 public:
  MetaCachedOperatorsTable();
  const std::string& name() const final;

 protected:
  friend class MetaTableManager;

  std::shared_ptr<Table> _on_generate() const final;

  void _process_pqp(const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash,
                    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes,
                    const std::shared_ptr<Table>& output_table) const;
};

}  // namespace opossum
