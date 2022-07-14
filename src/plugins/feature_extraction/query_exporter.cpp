#include "query_exporter.hpp"

namespace opossum {

std::string QueryExporter::query_hash(const std::string& query) {
  std::stringstream query_hex_hash;
  query_hex_hash << std::hex << std::hash<std::string>{}(query);
  // auto query_single_line{query};
  // query_single_line.erase(std::remove(query_single_line.begin(), query_single_line.end(), '\n'),
  // query_single_line.end());
  return query_hex_hash.str();
}

void QueryExporter::add_query(const std::string& hash, const std::string& query, const size_t frequency) {
  _queries.emplace_back(hash, query, frequency);
}

void QueryExporter::export_queries(const std::string& file_name) {
  std::cout << "export queries to " << file_name << std::endl;
}

}  // namespace opossum
