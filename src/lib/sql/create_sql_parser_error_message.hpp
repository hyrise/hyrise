#pragma once

#include <string>

#include "SQLParser.h"

namespace opossum {

std::string create_sql_parser_error_message(const std::string& sql, const hsql::SQLParserResult& result);

}  // namespace opossum
