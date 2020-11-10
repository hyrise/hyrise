
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "nlohmann/json.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"

namespace opossum {

// Shamelessly copied from tpcds_benchmark.cpp
const std::set<std::string> tpcds_filename_whitelist();

void _extract_get_tables(const std::shared_ptr<const AbstractOperator> pqp_node,
                         std::set<std::shared_ptr<const GetTable>>& get_table_operators);

const nlohmann::json _compute_pruned_chunks_per_table();

bool _extract_table_scans(const std::shared_ptr<const AbstractOperator> pqp_node,
                          std::map<std::string, std::vector<std::shared_ptr<const TableScan>>>& table_scans);

const nlohmann::json _compute_skipped_chunks_per_table();

const nlohmann::json _read_clustering_information();

void _append_additional_statistics(const std::string& result_file_path);

void _merge_result_files(const std::string& merge_result_file_name,
                         const std::vector<std::string>& merge_input_file_names, bool delete_files = true);

}  // namespace opossum
