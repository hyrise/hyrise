#include <iostream>
#include <SQLParserResult.h>
#include <SQLParser.h>
#include <sql/sql_query_plan.hpp>
#include <sql/sql_translator.hpp>
#include <sql/sql_result_operator.hpp>
#include <logical_query_plan/lqp_translator.hpp>
#include <scheduler/current_scheduler.hpp>
#include <utils/load_table.hpp>
#include <storage/chunk.hpp>
#include <storage/storage_manager.hpp>
#include <sql/sql_query_operator.hpp>
#include <sql/sql_pipeline.hpp>
#include <concurrency/transaction_manager.hpp>

using Matrix = std::vector<std::vector<opossum::AllTypeVariant>>;

Matrix _table_to_matrix(const std::shared_ptr<const opossum::Table>& table) {
  // initialize matrix with table sizes, including column names/types
  Matrix matrix(table->row_count() + 2, std::vector<opossum::AllTypeVariant>(table->column_count()));

  // set column names/types
  for (auto column_id = opossum::ColumnID{0}; column_id < table->column_count(); ++column_id) {
    matrix[0][column_id] = table->column_name(column_id);
    matrix[1][column_id] = "int";
  }

  // set values
  unsigned row_offset = 0;
  for (auto chunk_id = opossum::ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto chunk = table->get_chunk(chunk_id);

    // an empty table's chunk might be missing actual columns
    if (chunk->size() == 0) continue;

    for (auto column_id = opossum::ColumnID{0}; column_id < table->column_count(); ++column_id) {
      const auto column = chunk->get_column(column_id);

      for (auto chunk_offset = opossum::ChunkOffset{0}; chunk_offset < chunk->size(); ++chunk_offset) {
        matrix[row_offset + chunk_offset + 2][column_id] = (*column)[chunk_offset];
      }
    }
    row_offset += chunk->size();
  }

  return matrix;
}

int main() {

  auto& storage_manager = opossum::StorageManager::get();
  auto table = opossum::load_table("src/test/tables/int_int_int.tbl", opossum::Chunk::MAX_SIZE);
  storage_manager.add_table("items", table);

//  auto prepare_query = "PREPARE x FROM 'INSERT INTO items VALUES (?, ?, ?)'";
//  
//  opossum::SQLQueryOperator test_prp(prepare_query, false);
//  test_prp.execute();
//  
//  auto execute = "EXECUTE x(1, 2, 3)";
//  opossum::SQLQueryOperator test_ex(execute, false);
//  test_ex.execute();

  {
    auto sql_pipeline = std::make_unique<opossum::SQLPipeline>("SELECT * FROM items");
    auto matrix = _table_to_matrix(sql_pipeline->get_result_table());

    std::cout << std::endl;
    std::cout << "Before" << std::endl;
    std::cout << matrix.size() << std::endl;
  }


  auto query = "INSERT INTO items VALUES (?, ?, ?)";
  
  std::shared_ptr<hsql::SQLParserResult> result = std::make_shared<hsql::SQLParserResult>();
  hsql::SQLParser::parseSQLString(query, result.get());
  
  opossum::SQLQueryPlan _plan;
  _plan.set_num_parameters(3);

  const std::vector<hsql::SQLStatement*>& statements = result->getStatements();
  for (const hsql::SQLStatement* stmt : statements) {

    auto result_node = opossum::SQLTranslator{false}.translate_statement(*stmt);
    auto result_operator = opossum::LQPTranslator{}.translate_node(result_node);

    opossum::SQLQueryPlan query_plan;
    query_plan.add_tree_by_root(result_operator);

    _plan.append_plan(query_plan);
  }

  std::vector<opossum::AllParameterVariant> arguments = { 0, 1, 2 };

  opossum::SQLQueryPlan super_plan;
  const opossum::SQLQueryPlan plan = _plan.recreate(arguments);
  super_plan.append_plan(plan);

  auto tx = opossum::TransactionManager::get().new_transaction_context();
  super_plan.set_transaction_context(tx);
  
  auto _result_op = std::make_shared<opossum::SQLResultOperator>();
  auto _result_task = std::make_shared<opossum::OperatorTask>(_result_op);

  std::vector<std::shared_ptr<opossum::OperatorTask>> tasks = super_plan.create_tasks();
  if (tasks.size() > 0) {
    _result_op->set_input_operator(tasks.back()->get_operator());
    tasks.back()->set_as_predecessor_of(_result_task);
  }
  tasks.push_back(_result_task);
  
  opossum::CurrentScheduler::schedule_and_wait_for_tasks(tasks);
  
  tx->commit();

  {
    auto sql_pipeline = std::make_unique<opossum::SQLPipeline>("SELECT * FROM items");
    auto matrix = _table_to_matrix(sql_pipeline->get_result_table());
    
    std::cout << std::endl;
    std::cout << "After" << std::endl;
    std::cout << matrix.size() << std::endl;
  }
  
  return 0;
}
