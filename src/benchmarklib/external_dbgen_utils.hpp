#pragma once

#include <string>
#include <vector>

namespace hyrise {

// Some benchmarks, such as JCC-H or SSB, are offsprings of TPC-H in some manner. These benchmarks provide their own
// data and/or query generators. However, since the source files originate from the TPC-H dbgen/qgen, we cannot
// integrate them directly due to conflicting definitions or linking errors. Thus, we build their data/query generators
// as standalone executables, call them via CLI and load the tables into Hyrise.

/**
 * Calls the `dbgen` executable located at @param dbgen_path and copies the CSV meta JSONs from @param csv_meta_path to
 * @param tables_path for each table in @param table_names. dbgen is called with @param scale_factor and @param
 * additional_cli_args.
 */
void generate_csv_tables_with_external_dbgen(const std::string& dbgen_path, const std::vector<std::string>& table_names,
                                             const std::string& csv_meta_path, const std::string& tables_path,
                                             const float scale_factor, const std::string& additional_cli_args = "");

/**
 * Removes all CSV files at @param tables_path to free up some space.
 */
void remove_csv_tables(const std::string& tables_path);

}  // namespace hyrise
