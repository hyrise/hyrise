#include <iostream>

#include "types.hpp"
#include "operators/limit.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"

using namespace opossum;  // NOLINT

int main() {
  tpcc::TpccTableGenerator generator{10'000};
  const auto customer = generator.generate_customer_table();
  const auto tw = std::make_shared<TableWrapper>(customer);
  tw->execute();
  const auto l = std::make_shared<Limit>(tw, 100);
  l->execute();

  StorageManager::get().add_table("CUSTOMER", customer);

  Print::print(l->get_output());

  auto statement = SQLPipelineBuilder{"select * from CUSTOMER where C_SINCE > 5 LIMIT 2;"}.create_pipeline_statement();

  Print::print(statement.get_result_table());

  return 0;
}
