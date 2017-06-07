#include "sql_query_operator.hpp"

#include <memory>
#include <sstream>
#include <string>

#include "SQLParser.h"

namespace opossum {

using hsql::SQLParser;
using hsql::SQLParserResult;

// Runtime statistics.
std::atomic<size_t> SQLQueryOperator::num_executed(0);
std::atomic<size_t> SQLQueryOperator::parse_tree_cache_hits(0);
std::atomic<size_t> SQLQueryOperator::parse_tree_cache_misses(0);

// Caches
SQLParseTreeCache SQLQueryOperator::_parse_tree_cache(0);
SQLParseTreeCache SQLQueryOperator::_prepared_stmts(1024);

SQLQueryOperator::SQLQueryOperator(const std::string& query, bool schedule_plan)
    : _query(query), _schedule_plan(schedule_plan) {
  _result_op = std::make_shared<SQLResultOperator>();
  _result_task = std::make_shared<OperatorTask>(_result_op);
}

const std::string SQLQueryOperator::name() const { return "SQLQueryOperator"; }

uint8_t SQLQueryOperator::num_in_tables() const { return 0; }

uint8_t SQLQueryOperator::num_out_tables() const { return 0; }

const std::shared_ptr<OperatorTask>& SQLQueryOperator::get_result_task() const { return _result_task; }

const SQLQueryPlan& SQLQueryOperator::get_query_plan() const { return _plan; }

std::shared_ptr<const Table> SQLQueryOperator::on_execute(std::shared_ptr<TransactionContext> context) {
  // TODO(torpedro): Check query cache for execution plan.
  ++num_executed;

  std::shared_ptr<SQLParserResult> parse_result = parse_query(_query);

  // Populates the query plan in _plan.
  compile_parse_result(parse_result);

  // Schedule all tasks in query plan.
  if (_schedule_plan) {
    auto tasks = _plan.tasks();

    if (tasks.size() > 0) {
      tasks.back()->set_as_predecessor_of(_result_task);
      _result_op->set_input_operator(tasks.back()->get_operator());

      for (const auto& task : tasks) {
        task->schedule();
      }
      _result_task->schedule();
    }
  }

  return nullptr;
}

std::shared_ptr<SQLParserResult> SQLQueryOperator::parse_query(const std::string& query) const {
  // Check query cache for parse tree.
  if (_parse_tree_cache.has(_query)) {
    ++parse_tree_cache_hits;
    return _parse_tree_cache.get(_query);
  }
  ++parse_tree_cache_misses;

  // Parse the query.
  std::shared_ptr<SQLParserResult> result = std::make_shared<SQLParserResult>();

  SQLParser::parseSQLString(query, result.get());

  if (!result->isValid()) {
    std::stringstream error_msg;
    error_msg << "SQL Parsing failed: " << result->errorMsg();
    error_msg << " (L" << result->errorLine() << ":" << result->errorColumn() << ")";
    throw error_msg;
  }

  // Add the result to the cache.
  _parse_tree_cache.set(_query, result);

  return result;
}

// Translates the query that is supposed to be prepared and saves it
// in the prepared statement cache by its name.
void SQLQueryOperator::prepare_statement(const hsql::PrepareStatement& prepare_stmt) {
  std::shared_ptr<SQLParserResult> result = parse_query(prepare_stmt.query);

  // Cache the result.
  _prepared_stmts.set(prepare_stmt.name, result);
}

// Tries to fetch the referenced prepared statement and retrieve its cached data.
void SQLQueryOperator::execute_prepared_statement(const hsql::ExecuteStatement& execute_stmt) {
  if (!_prepared_stmts.has(execute_stmt.name)) {
    throw "Requested prepared statement does not exist!";
  }

  std::shared_ptr<SQLParserResult> parse_result = _prepared_stmts.get(execute_stmt.name);
  compile_parse_result(parse_result);
}

// Translate the statement and append the result plan
// to the current total query plan (in member _plan).
void SQLQueryOperator::plan_statement(const hsql::SQLStatement& stmt) {
  SQLQueryTranslator translator;

  if (!translator.translate_statement(stmt)) {
    throw translator.get_error_msg();
  }

  _plan.append(translator.get_query_plan());
}

// Compiles the given parse result into an operator plan.
void SQLQueryOperator::compile_parse_result(std::shared_ptr<SQLParserResult> result) {
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
        plan_statement(*stmt);
        break;
      }
    }
  }
}

// static
SQLParseTreeCache& SQLQueryOperator::get_parse_tree_cache() { return _parse_tree_cache; }

}  // namespace opossum
