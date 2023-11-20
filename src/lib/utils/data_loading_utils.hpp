#pragma once

#include <string>

#include "types.hpp"

namespace hyrise {

namespace data_loading_utils {

void load_column_when_necessary(const std::string& table_name, const ColumnID column_id);

void wait_for_table(const std::string& success_log_message);

}  // namespace data_loading_utils

}  // namespace hyrise
