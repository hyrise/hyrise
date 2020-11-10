#include "util.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

// Shamelessly copied from tpcds_benchmark.cpp
const std::set<std::string> tpcds_filename_whitelist() {
  auto filename_whitelist = std::set<std::string>{};
  const auto blacklist_file_path = "resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0 && filename.at(0) == '#') {
        filename_whitelist.emplace(filename.substr(1));
      }
    }
    blacklist_file.close();
  }
  return filename_whitelist;
}

void _extract_get_tables(const std::shared_ptr<const AbstractOperator> pqp_node,
                         std::set<std::shared_ptr<const GetTable>>& get_table_operators) {
  if (pqp_node->type() == OperatorType::GetTable) {
    auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp_node);
    Assert(get_table_op, "could not cast to GetTable");
    get_table_operators.insert(get_table_op);
  } else {
    if (pqp_node->left_input()) _extract_get_tables(pqp_node->left_input(), get_table_operators);
    if (pqp_node->right_input()) _extract_get_tables(pqp_node->right_input(), get_table_operators);
  }
}

const nlohmann::json _compute_pruned_chunks_per_table() {
  std::map<std::string, std::vector<size_t>> pruned_chunks_per_table;

  const auto cache_snapshot = Hyrise::get().default_pqp_cache->snapshot();
  for (const auto& [query_string, snapshot_entry] : cache_snapshot) {
    const auto& physical_query_plan = snapshot_entry.value;
    const auto& frequency = snapshot_entry.frequency;
    Assert(frequency, "Optional frequency is unexpectedly not set.");

    std::set<std::shared_ptr<const GetTable>> get_table_operators;
    _extract_get_tables(physical_query_plan, get_table_operators);

    for (const auto& get_table : get_table_operators) {
      const auto& table_name = get_table->table_name();
      const auto& number_of_pruned_chunks = get_table->pruned_chunk_ids().size();
      for (size_t run{0}; run < *frequency; run++) {
        pruned_chunks_per_table[table_name].push_back(number_of_pruned_chunks);
      }
    }
  }

  return pruned_chunks_per_table;
}

bool _extract_table_scans(const std::shared_ptr<const AbstractOperator> pqp_node,
                          std::map<std::string, std::vector<std::shared_ptr<const TableScan>>>& table_scans) {
  // we want only scans that happen before joins, and on permanent columns
  // to filter those out, we need to walk down the entire PQP recursively
  // on the way back (i.e., up the PQP), use the return values to decide whether table scans above should be considered

  bool left_input_ignores = false;
  bool right_input_ignores = false;

  if (pqp_node->left_input()) left_input_ignores = _extract_table_scans(pqp_node->left_input(), table_scans);
  if (pqp_node->right_input()) right_input_ignores = _extract_table_scans(pqp_node->right_input(), table_scans);

  // some input below could already be "illegal"
  if (left_input_ignores || right_input_ignores) return true;

  // this operator could be "illegal"
  std::vector<std::string> forbidden_words = {"ColumnVsColumn", "SUBQUERY", "SUM", "AVG", "COUNT"};
  const auto& description = pqp_node->description();
  for (const auto& forbidden_word : forbidden_words) {
    if (description.find(forbidden_word) != std::string::npos) return true;
  }

  // this operator is interesting
  // If it is a table scan, next find out the table it belongs to, and store it

  if (pqp_node->type() == OperatorType::TableScan) {
    auto op = pqp_node;
    while (op->type() != OperatorType::GetTable) {
      op = op->left_input();
      Assert(op, "reached a node with no input, without reaching a GetTable");
    }
    auto get_table = std::dynamic_pointer_cast<const GetTable>(op);
    Assert(get_table, "could not cast to GetTable");
    const auto& table_name = get_table->table_name();

    auto table_scan = std::dynamic_pointer_cast<const TableScan>(pqp_node);
    Assert(table_scan, "could not cast to TableScan");
    table_scans[table_name].push_back(table_scan);
  }

  // scans above might still be interesting
  return false;
}

const nlohmann::json _compute_skipped_chunks_per_table() {
  std::map<std::string, std::vector<size_t>> skipped_chunks_per_table;

  const auto cache_snapshot = Hyrise::get().default_pqp_cache->snapshot();
  for (const auto& [query_string, snapshot_entry] : cache_snapshot) {
    const auto& physical_query_plan = snapshot_entry.value;
    const auto& frequency = snapshot_entry.frequency;
    Assert(frequency, "Optional frequency is unexpectedly not set.");

    std::map<std::string, std::vector<std::shared_ptr<const TableScan>>> table_scans;
    _extract_table_scans(physical_query_plan, table_scans);

    for (const auto& [table_name, table_scans] : table_scans) {
      for (const auto& table_scan : table_scans) {
        Assert(dynamic_cast<TableScan::PerformanceData*>(table_scan->performance_data.get()),
               "performance data was not of type TableScanPerformanceData");
        const auto& table_scan_performance_data =
            dynamic_cast<TableScan::PerformanceData&>(*table_scan->performance_data);

        for (size_t run{0}; run < *frequency; run++) {
          skipped_chunks_per_table[table_name].push_back(table_scan_performance_data.chunk_scans_skipped);
        }
      }
    }
  }

  return skipped_chunks_per_table;
}

const nlohmann::json _read_clustering_information() {
  const std::string filename = ".clustering_info.json";
  if (!std::filesystem::exists(filename)) {
    std::cout << "clustering info file not found: " << filename << std::endl;
    std::exit(1);
  }

  std::ifstream ifs(filename);
  const auto clustering_info = nlohmann::json::parse(ifs);
  return clustering_info;
}

void _append_additional_statistics(const std::string& result_file_path) {
  std::ifstream benchmark_result_file(result_file_path);
  auto benchmark_result_json = nlohmann::json::parse(benchmark_result_file);

  const auto benchmark_count = benchmark_result_json["benchmarks"].size();
  Assert(benchmark_count == 1, "expected " + result_file_path +
                                   " file containing exactly one benchmark, but it contains " +
                                   std::to_string(benchmark_count));
  const std::string query_name = benchmark_result_json["benchmarks"].at(0)["name"];

  benchmark_result_json["pruning_stats"][query_name] = _compute_pruned_chunks_per_table();
  benchmark_result_json["skipped_chunk_stats"][query_name] = _compute_skipped_chunks_per_table();

  // write results back
  std::ofstream final_result_file(result_file_path);
  final_result_file << benchmark_result_json.dump(2) << std::endl;
  final_result_file.close();
}

void _merge_result_files(const std::string& merge_result_file_name,
                         const std::vector<std::string>& merge_input_file_names, bool delete_files) {
  Assert(!merge_input_file_names.empty(), "you have to provide file names to merge");
  nlohmann::json merge_result_json;

  for (const auto& file_name : merge_input_file_names) {
    std::ifstream benchmark_result_file(file_name);
    auto benchmark_result_json = nlohmann::json::parse(benchmark_result_file);
    const auto benchmark_count = benchmark_result_json["benchmarks"].size();
    Assert(benchmark_count == 1, "expected " + file_name + " file containing exactly one benchmark, but it contains " +
                                     std::to_string(benchmark_count));
    const auto pruning_stats_count = benchmark_result_json["pruning_stats"].size();
    Assert(pruning_stats_count == 1, "expected " + file_name +
                                         " file containing exactly pruning stats for just one query, but it contains " +
                                         std::to_string(pruning_stats_count));

    if (merge_result_json.empty()) {
      merge_result_json = benchmark_result_json;
    } else {
      const auto benchmark = benchmark_result_json["benchmarks"].at(0);
      const std::string query_name = benchmark["name"];
      merge_result_json["benchmarks"].push_back(benchmark);
      merge_result_json["pruning_stats"][query_name] = benchmark_result_json["pruning_stats"][query_name];
      merge_result_json["skipped_chunk_stats"][query_name] = benchmark_result_json["skipped_chunk_stats"][query_name];
    }
  }

  // store clustering info - config, algo, runtimes
  const auto clustering_info_json = _read_clustering_information();
  merge_result_json["clustering_info"] = clustering_info_json;

  if (delete_files) {
    for (const auto& path : merge_input_file_names) {
      Assert(!std::remove(path.c_str()), "could not remove " + path.c_str());
    }
  }

  // write results back
  std::ofstream final_result_file(merge_result_file_name);
  final_result_file << merge_result_json.dump(2) << std::endl;
  final_result_file.close();
}

}  // namespace opossum