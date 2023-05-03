#pragma once

#include <string>

namespace hyrise {

void generate_csv_tables_with_dbgen(const std::string& dbgen_path, const std::vector<std::string>& table_names,
                                    const std::string& csv_meta_path, const std::string& tables_path,
                                    const float scale_factor, const std::string& args);

void remove_csv_tables(const std::string& tables_path);

}  // namespace hyrise
