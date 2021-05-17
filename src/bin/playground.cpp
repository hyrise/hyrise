#include <iostream>

#include "re2/re2.h"

#include "types.hpp"


#include "benchmark_config.hpp"
#include "cli_config_parser.hpp"
#include "server/server.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"

#include "sql/sql_pipeline_builder.hpp"


using namespace opossum;  // NOLINT

int main() {
  auto config = std::make_shared<opossum::BenchmarkConfig>(opossum::BenchmarkConfig::get_default_config());
  config->cache_binary_tables = true;
  TPCHTableGenerator{1.0f, ClusteringConfiguration::None, config}.generate_and_store();

  std::cout << re2::RE2::FullMatch("USA", "Germany|USA") << std::endl;

  auto district_pipeline = SQLPipelineBuilder{std::string{"SELECT count(*) FROM lineitem WHERE l_shipdate IN ('2001', '2002', '2003', '2004')"}}.create_pipeline();
  const auto [pipeline_status, table] = district_pipeline.get_result_table();
  std::cout << table->row_count() << std::endl;
  return 0;
}
