#include "sql_query_operator.hpp"

#include <memory>
#include <string>

#include "SQLParser.h"

namespace opossum {

SQLParseTreeCache SQLQueryOperator::_parse_tree_cache = SQLParseTreeCache(0);

SQLParseTreeCache SQLQueryOperator::_prepared_stmts = SQLParseTreeCache(1024);

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

  std::shared_ptr<hsql::SQLParserResult> parse_result = parse_query(_query);

  compile_parse_result(parse_result);

  return nullptr;
}

std::shared_ptr<hsql::SQLParserResult> SQLQueryOperator::parse_query(const std::string& query) {
  // Check query cache for parse tree.
  if (_parse_tree_cache.has(_query)) {
    return _parse_tree_cache.get(_query);
  }

  // Parse the query.
  std::shared_ptr<hsql::SQLParserResult> result = std::make_shared<hsql::SQLParserResult>();

  // TODO: Move parse logic from translator into this operator
  if (!_translator.parse_query(_query, result.get())) {
    throw _translator.get_error_msg();
  }

  return result;
}

// Translates the query that is supposed to be prepared and saves it
// in the prepared statement cache by its name.
void SQLQueryOperator::prepare_statement(const hsql::PrepareStatement& prepare_stmt) {
  SQLQueryTranslator translator;

  std::shared_ptr<hsql::SQLParserResult> result;
  result = std::make_shared<hsql::SQLParserResult>();
  if (!translator.parse_query(prepare_stmt.query, result.get())) {
    throw translator.get_error_msg();
  }

  // Cache the result.
  _prepared_stmts.set(prepare_stmt.name, result);
}

// Tries to fetch the referenced prepared statement and retrieve its cached data.
void SQLQueryOperator::execute_prepared_statement(const hsql::ExecuteStatement& execute_stmt) {
  if (!_prepared_stmts.has(execute_stmt.name)) {
    throw "Requested prepared statement does not exist!";
  }

  std::shared_ptr<hsql::SQLParserResult> parse_result = _prepared_stmts.get(execute_stmt.name);
  compile_parse_result(parse_result);
}

// Compiles the given parse result into an operator plan.
void SQLQueryOperator::compile_parse_result(std::shared_ptr<hsql::SQLParserResult> result) {
  SQLQueryTranslator translator;
  const std::vector<hsql::SQLStatement*>& statements = result->getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    switch (stmt->type()) {
      case hsql::kStmtPrepare:
        prepare_statement((const hsql::PrepareStatement&)*stmt);
        break;
      case hsql::kStmtExecute:
        execute_prepared_statement((const hsql::ExecuteStatement&)*stmt);
        break;
      default: {
        if (!translator.translate_statement(*stmt)) {
          throw translator.get_error_msg();
        }
        break;
      }
    }
  }

  // Schedule all tasks.
  const SQLQueryPlan& plan = translator.get_query_plan();
  auto tasks = plan.tasks();

  if (tasks.size() > 0) {
    tasks.back()->set_as_predecessor_of(_result_task);
    _result_op->set_input_operator(tasks.back()->get_operator());

    for (const auto& task : tasks) {
      task->schedule();
    }
    _result_task->schedule();
  }
}

}  // namespace opossum
