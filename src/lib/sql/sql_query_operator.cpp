#include "sql_query_operator.hpp"

#include <memory>
#include <string>

#include "sql_query_translator.hpp"

#include "SQLParser.h"

namespace opossum {

SQLQueryOperator::SQLQueryOperator(const std::string& query) : _query(query) {
  _result_op = std::make_shared<SQLResultOperator>();
  _result_task = std::make_shared<OperatorTask>(_result_op);
}

const std::string SQLQueryOperator::name() const { return "SQLQueryOperator"; }

uint8_t SQLQueryOperator::num_in_tables() const { return 0; }

uint8_t SQLQueryOperator::num_out_tables() const { return 0; }

const std::shared_ptr<OperatorTask>& SQLQueryOperator::get_result_task() const { return _result_task; }

std::shared_ptr<const Table> SQLQueryOperator::on_execute(std::shared_ptr<TransactionContext> context) {
  // TODO(torpedro): Check query cache for execution plan.

  // TODO(torpedro): Check query cache for syntax tree.

  SQLQueryTranslator translator;

  hsql::SQLParserResult result;

  if (!translator.parse_query(_query, &result)) {
    throw translator.get_error_msg();
  }

  // Translate the query.
  if (!translator.translate_parse_result(result)) {
    throw translator.get_error_msg();
  }

  // Schedule all tasks.
  auto tasks = translator.get_tasks();

  tasks.back()->set_as_predecessor_of(_result_task);
  _result_op->set_input_operator(tasks.back()->get_operator());

  for (const auto& task : tasks) {
    task->schedule();
  }
  _result_task->schedule();

  return NULL;
}

}  // namespace opossum
