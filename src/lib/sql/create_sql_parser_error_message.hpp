#pragma once


#include "SQLParser.h" // NEEDEDINCLUDE

namespace opossum {

std::string create_sql_parser_error_message(const std::string& sql, const hsql::SQLParserResult& result);

}  // namespace opossum
