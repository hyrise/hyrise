#include "meta_cached_operators_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaCachedOperatorsTable::MetaCachedOperatorsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"operator", DataType::String, false},
                                               {"query_hash", DataType::String, false},
                                               {"description", DataType::String, false},
                                               {"walltime_ns", DataType::Long, false},
                                               {"output_chunks", DataType::Long, false},
                                               {"output_rows", DataType::Long, false}}) {}

const std::string& MetaCachedOperatorsTable::name() const {
  static const auto name = std::string{"cached_operators"};
  return name;
}

std::shared_ptr<Table> MetaCachedOperatorsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  if (!Hyrise::get().default_pqp_cache) return output_table;

  const auto cache_map = Hyrise::get().default_pqp_cache->snapshot();

  for (const auto& [query_string, entry] : cache_map) {
    std::stringstream query_hex_hash;
    query_hex_hash << std::hex << std::hash<std::string>{}(query_string);

    std::unordered_set<std::shared_ptr<const AbstractOperator>> visited_pqp_nodes;
    _process_pqp(entry.value, query_hex_hash.str(), visited_pqp_nodes, output_table);
  }

  return output_table;
}

void MetaCachedOperatorsTable::_process_pqp(
    const std::shared_ptr<const AbstractOperator>& op, const std::string& query_hex_hash,
    std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_pqp_nodes,
    const std::shared_ptr<Table>& output_table) const {
  const auto& performance_data = op->performance_data();
  output_table->append({pmr_string{op->name()}, pmr_string{query_hex_hash}, pmr_string{op->description()},
                        static_cast<int64_t>(performance_data.walltime.count()),
                        static_cast<int64_t>(performance_data.output_chunk_count),
                        static_cast<int64_t>(performance_data.output_row_count)});

  visited_pqp_nodes.insert(op);

  const auto left_input = op->input_left();
  const auto right_input = op->input_right();
  if (left_input && !visited_pqp_nodes.contains(left_input)) {
    _process_pqp(left_input, query_hex_hash, visited_pqp_nodes, output_table);
  }
  if (right_input && !visited_pqp_nodes.contains(right_input)) {
    _process_pqp(right_input, query_hex_hash, visited_pqp_nodes, output_table);
  }
}

}  // namespace opossum
