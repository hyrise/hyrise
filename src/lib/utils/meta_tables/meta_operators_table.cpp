#include "meta_operators_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaOperatorsTable::MetaOperatorsTable()
    : AbstractMetaTable(
          TableColumnDefinitions{{"query_hash", DataType::String, false}, {"operator", DataType::String, false}}) {}

const std::string& MetaOperatorsTable::name() const {
  static const auto name = std::string{"cached_operators"};
  return name;
}

std::shared_ptr<Table> MetaOperatorsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  if (!Hyrise::get().default_pqp_cache) return output_table;

  for (auto iter = Hyrise::get().default_pqp_cache->unsafe_begin();
       iter != Hyrise::get().default_pqp_cache->unsafe_end(); ++iter) {
    const auto& [query_string, physical_query_plan] = *iter;
    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_pqp_nodes;
    _process_pqp(physical_query_plan, query_hex_hash.str(), visited_pqp_nodes, output_table);
  }

  return output_table;
}

void MetaOperatorsTable::_process_pqp(const std::shared_ptr<const AbstractOperator>& op,
                                      const std::string& query_hex_hash,
                                      std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes,
                                      const std::shared_ptr<Table>& output_table) const {
  output_table->append({pmr_string{query_hex_hash}, pmr_string{op->name()}});

  visited_pqp_nodes.insert(op);

  const auto left_input = op->input_left();
  const auto right_input = op->input_right();
  if (left_input && !visited_pqp_nodes.contains(left_input)) {
    _process_pqp(left_input, query_hex_hash, visited_pqp_nodes, output_table);
    visited_pqp_nodes.insert(std::move(left_input));
  }
  if (right_input && !visited_pqp_nodes.contains(right_input)) {
    _process_pqp(right_input, query_hex_hash, visited_pqp_nodes, output_table);
    visited_pqp_nodes.insert(std::move(right_input));
  }
}

}  // namespace opossum
