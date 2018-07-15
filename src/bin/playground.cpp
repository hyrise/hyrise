#include <iostream>

#include "types.hpp"

#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "operators/print.hpp"

using namespace opossum;  // NOLINT

int main() {
  TpchDbGenerator{0.01, 10'000}.generate_and_store();

  auto create_view_statement = SQLPipelineBuilder(R"(create view revenue (supplier_no, total_revenue) as SELECT l_suppkey,
  SUM(l_extendedprice * (1.0 - l_discount)) FROM lineitem WHERE l_shipdate >= '1993-05-13'
  AND l_shipdate < '1993-08-13' GROUP BY l_suppkey ORDER BY l_suppkey;)").disable_mvcc().create_pipeline_statement();


  auto query_statement = SQLPipelineBuilder(R"(SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue
      WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue)
      FROM revenue) ORDER BY s_suppkey;)").disable_mvcc().create_pipeline_statement();

  create_view_statement.get_result_table();

  Print::print(SQLPipelineBuilder("SELECT * FROM revenue;").disable_mvcc().create_pipeline_statement().get_result_table());
  Print::print(query_statement.get_result_table());

  return 0;
}
