#include "query_exporter.hpp"

#include <fstream>

namespace hyrise {

QueryExporter::QueryExporter()
    : _file_name{std::make_shared<FileName>("FeatureExtractionPlugin.QueryExporter.FileName")} {
  _file_name->register_at_settings_manager();
}

QueryExporter::~QueryExporter() {
  _file_name->unregister_at_settings_manager();
}

void QueryExporter::add_query(const std::shared_ptr<Query>& query) {
  _queries.push_back(query);
}

void QueryExporter::export_queries(const std::string& file_path) {
  const auto file_name = file_path + "/" + _file_name->get();
  std::cout << "export queries to " << file_name << std::endl;
  auto output_file = std::ofstream{file_name};
  Assert(output_file.good(), "Cannot write to " + file_name);
  output_file << "query_hash;query;frequency\n";
  for (const auto& query : _queries) {
    auto query_single_line{query->query};
    query_single_line.erase(std::remove(query_single_line.begin(), query_single_line.end(), '\n'),
                            query_single_line.end());
    // std::cout << query_single_line << std::endl;
    // add escapes to query to allow for ; at end of statements
    output_file << query->hash << ";\"" << query_single_line << "\";" << query->frequency << "\n";
  }
  output_file.flush();
  Assert(output_file.good(), "Error occurred when writing to " + file_name);
  output_file.close();
}

QueryExporter::FileName::FileName(const std::string& init_name) : AbstractSetting(init_name) {}

const std::string& QueryExporter::FileName::description() const {
  static const auto description = std::string{"Output file for the queries"};
  return description;
}

const std::string& QueryExporter::FileName::get() {
  return _value;
}

void QueryExporter::FileName::set(const std::string& value) {
  _value = value;
}

}  // namespace hyrise
