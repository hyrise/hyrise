#include "abstract_tpcc_procedure.hpp"

#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

// TODO Move context into overloaded execute so that tx becomes shorter?
AbstractTpccProcedure::AbstractTpccProcedure() : _transaction_context(TransactionManager::get().new_transaction_context()) {}

std::shared_ptr<const Table> AbstractTpccProcedure::_execute_sql(std::string sql) {
  // std::cout << sql << std::endl;
  auto builder = SQLPipelineBuilder{sql};
  builder.with_transaction_context(_transaction_context);
  auto pipeline = builder.create_pipeline();
  return pipeline.get_result_table();
}

std::ostream& operator<<(std::ostream& stream, const AbstractTpccProcedure& procedure) {
  return procedure.print(stream);
}

thread_local std::minstd_rand AbstractTpccProcedure::_random_engine = std::minstd_rand{};
thread_local TpccRandomGenerator AbstractTpccProcedure::_tpcc_random_generator = TpccRandomGenerator{42};

}