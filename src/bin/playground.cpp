#include <iostream>
#include <optimizer/abstract_syntax_tree/abstract_ast_node.hpp>
#include <sql/sql_to_ast_translator.hpp>
#include <storage/storage_manager.hpp>
#include <utils/load_table.hpp>

#include "SQLParser.h"
#include "SQLParserResult.h"

int main() {
  opossum::StorageManager::get().add_table("groupby_int_2gb_2agg", opossum::load_table("src/test/tables/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 0));

  auto query = "SELECT a, b, MAX(c), AVG(d) FROM groupby_int_2gb_2agg GROUP BY a, b HAVING b > 457 OR b = 1234 OR b = 12345;";

  hsql::SQLParserResult result;
  hsql::SQLParser::parse(query, &result);

  auto nodes = opossum::SQLToASTTranslator(false).translate_parse_result(result);



  nodes[0]->print();

  return 0;
}
