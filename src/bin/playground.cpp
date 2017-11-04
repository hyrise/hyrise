#include <iostream>

#include <chrono>
#include <planviz/ast_visualizer.hpp>
#include <optimizer/optimizer.hpp>

#include "sql/sql_to_ast_translator.hpp"
#include "operators/import_csv.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "scheduler/topology.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/abstract_scheduler.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "utils/load_table.hpp"

#include "tpch/tpch_queries.hpp"
#include "SQLParser.h"
#include "SQLParserResult.h"

int main() {
  std::shared_ptr<opossum::Table> customer = opossum::load_table("src/test/tables/tpch/customer.tbl", 2);
  std::shared_ptr<opossum::Table> lineitem = opossum::load_table("src/test/tables/tpch/lineitem.tbl", 2);
  std::shared_ptr<opossum::Table> nation = opossum::load_table("src/test/tables/tpch/nation.tbl", 2);
  std::shared_ptr<opossum::Table> orders = opossum::load_table("src/test/tables/tpch/orders.tbl", 2);
  std::shared_ptr<opossum::Table> part = opossum::load_table("src/test/tables/tpch/part.tbl", 2);
  std::shared_ptr<opossum::Table> partsupplier = opossum::load_table("src/test/tables/tpch/partsupplier.tbl", 2);
  std::shared_ptr<opossum::Table> region = opossum::load_table("src/test/tables/tpch/region.tbl", 2);
  std::shared_ptr<opossum::Table> supplier = opossum::load_table("src/test/tables/tpch/supplier.tbl", 2);
  opossum::StorageManager::get().add_table("customer", std::move(customer));
  opossum::StorageManager::get().add_table("lineitem", std::move(lineitem));
  opossum::StorageManager::get().add_table("nation", std::move(nation));
  opossum::StorageManager::get().add_table("orders", std::move(orders));
  opossum::StorageManager::get().add_table("part", std::move(part));
  opossum::StorageManager::get().add_table("partsupp", std::move(partsupplier));
  opossum::StorageManager::get().add_table("region", std::move(region));
  opossum::StorageManager::get().add_table("supplier", std::move(supplier));

  hsql::SQLParserResult parser_result;
  hsql::SQLParser::parse(opossum::tpch_queries[6], &parser_result);

  opossum::DotConfig config;
  config.background_color = opossum::DotColor::Black;
  config.render_format = opossum::DotRenderFormat::SVG;

  auto ast = opossum::SQLToASTTranslator(false).translate_parse_result(parser_result);
  opossum::ASTVisualizer(ast, "vis", config).visualize();

  auto astopt = opossum::Optimizer::get().optimize(ast[0]);
  opossum::ASTVisualizer({astopt}, "visopt", config).visualize();

  return 0;
}
