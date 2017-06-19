#include "sql_query_operator.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "SQLParser.h"

namespace opossum {

using hsql::SQLStatement;
using hsql::PrepareStatement;
using hsql::ExecuteStatement;
using hsql::kStmtPrepare;
using hsql::kStmtExecute;
using hsql::SQLParser;
using hsql::SQLParserResult;

// Static.
// Query plan / parse tree caches.
SQLParseTreeCache SQLQueryOperator::_parse_tree_cache(0);
SQLParseTreeCache SQLQueryOperator::_prepared_stmts(1024);
SQLQueryPlanCache SQLQueryOperator::_query_plan_cache(0);

SQLQueryOperator::SQLQueryOperator(const std::string& query, bool schedule_plan)
    : _query(query), _schedule_plan(schedule_plan), _hit_parse_tree_cache(false), _hit_query_plan_cache(false) {
  _result_op = std::make_shared<SQLResultOperator>();
  _result_task = std::make_shared<OperatorTask>(_result_op);
}

const std::string SQLQueryOperator::name() const { return "SQLQueryOperator"; }

uint8_t SQLQueryOperator::num_in_tables() const { return 0; }

uint8_t SQLQueryOperator::num_out_tables() const { return 0; }

const std::shared_ptr<OperatorTask>& SQLQueryOperator::get_result_task() const { return _result_task; }

const SQLQueryPlan& SQLQueryOperator::get_query_plan() const { return _plan; }

bool SQLQueryOperator::hit_parse_tree_cache() const { return _hit_parse_tree_cache; }

bool SQLQueryOperator::hit_query_plan_cache() const { return _hit_query_plan_cache; }

std::shared_ptr<const Table> SQLQueryOperator::on_execute(std::shared_ptr<TransactionContext> context) {
  // Compile the query.
  compile_query(_query);

  // Schedule all tasks in query plan.
  if (_schedule_plan) {
    for (const auto& task : _plan.tasks()) {
      task->schedule();
    }
  }

  return nullptr;
}

void SQLQueryOperator::compile_query(const std::string& query) {
  // Check the query plan cache.
  SQLQueryPlan cached_plan;
  if (_query_plan_cache.try_get(_query, &cached_plan)) {
    _hit_query_plan_cache = true;
    _plan = cached_plan.recreate();
    return;
  }

  // parse the query.
  std::shared_ptr<SQLParserResult> parse_result = parse_query(_query);

  // Populates the query plan in _plan.
  compile_parse_result(parse_result);

  // Add the result task to the query plan.
  if (_plan.size() > 0) {
    _plan.back()->set_as_predecessor_of(_result_task);
    _result_op->set_input_operator(_plan.back()->get_operator());
    _plan.add_task(_result_task);
  }

  // Cache the plan.
  _query_plan_cache.set(_query, _plan);
}

std::shared_ptr<SQLParserResult> SQLQueryOperator::parse_query(const std::string& query) {
  std::shared_ptr<SQLParserResult> result = std::make_shared<SQLParserResult>();

  // Check parse tree cache.
  if (_parse_tree_cache.try_get(_query, &result)) {
    _hit_parse_tree_cache = true;
    return result;
  }

  _hit_parse_tree_cache = false;

  // Parse the query into our result object.
  SQLParser::parseSQLString(query, result.get());

  if (!result->isValid()) {
    std::stringstream error_msg;
    error_msg << "SQL Parsing failed: " << result->errorMsg();
    error_msg << " (L" << result->errorLine() << ":" << result->errorColumn() << ")";
    throw std::runtime_error(error_msg.str());
  }

  // Add the result to the cache.
  _parse_tree_cache.set(_query, result);

  return result;
}

// Translates the query that is supposed to be prepared and saves it
// in the prepared statement cache by its name.
void SQLQueryOperator::prepare_statement(const PrepareStatement& prepare_stmt) {
  std::shared_ptr<SQLParserResult> result = parse_query(prepare_stmt.query);

  // Cache the result.
  _prepared_stmts.set(prepare_stmt.name, result);
}

// Tries to fetch the referenced prepared statement and retrieve its cached data.
void SQLQueryOperator::execute_prepared_statement(const ExecuteStatement& execute_stmt) {
  std::shared_ptr<SQLParserResult> parse_result = std::make_shared<SQLParserResult>();

  if (!_prepared_stmts.try_get(execute_stmt.name, &parse_result)) {
    throw std::runtime_error("Requested prepared statement does not exist!");
  }

  compile_parse_result(parse_result);
}

// Translate the statement and append the result plan
// to the current total query plan (in member _plan).
void SQLQueryOperator::plan_statement(const SQLStatement& stmt) {
  SQLQueryTranslator translator;

  if (!translator.translate_statement(stmt)) {
    throw std::runtime_error(translator.get_error_msg());
  }

  _plan.append(translator.get_query_plan());
}

// Compiles the given parse result into an operator plan.
void SQLQueryOperator::compile_parse_result(std::shared_ptr<SQLParserResult> result) {
  const std::vector<SQLStatement*>& statements = result->getStatements();

  for (const SQLStatement* stmt : statements) {
    switch (stmt->type()) {
      case kStmtPrepare:
        prepare_statement((const PrepareStatement&)*stmt);
        break;
      case kStmtExecute:
        execute_prepared_statement((const ExecuteStatement&)*stmt);
        break;
      default: {
        plan_statement(*stmt);
        break;
      }
    }
  }
}

// Static.
SQLParseTreeCache& SQLQueryOperator::get_parse_tree_cache() { return _parse_tree_cache; }

// Static.
SQLQueryPlanCache& SQLQueryOperator::get_query_plan_cache() { return _query_plan_cache; }

}  // namespace opossum
