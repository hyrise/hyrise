#include "sql_query_operator.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "SQLParser.h"

#include "all_parameter_variant.hpp"
#include "optimizer/abstract_syntax_tree/ast_to_operator_translator.hpp"
#include "sql_query_plan.hpp"
#include "sql_to_ast_translator.hpp"

#include "utils/assert.hpp"

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
SQLQueryCache<std::shared_ptr<hsql::SQLParserResult>> SQLQueryOperator::_parse_tree_cache(0);
SQLQueryCache<SQLQueryPlan> SQLQueryOperator::_prepared_stmts(1024);
SQLQueryCache<SQLQueryPlan> SQLQueryOperator::_query_plan_cache(0);

SQLQueryOperator::SQLQueryOperator(const std::string& query, bool schedule_plan)
    : _query(query), _schedule_plan(schedule_plan), _parse_tree_cache_hit(false), _query_plan_cache_hit(false) {
  _result_op = std::make_shared<SQLResultOperator>();
  _result_task = std::make_shared<OperatorTask>(_result_op);
}

const std::string SQLQueryOperator::name() const { return "SQLQueryOperator"; }

uint8_t SQLQueryOperator::num_in_tables() const { return 0; }

uint8_t SQLQueryOperator::num_out_tables() const { return 0; }

const std::shared_ptr<OperatorTask>& SQLQueryOperator::get_result_task() const { return _result_task; }

const SQLQueryPlan& SQLQueryOperator::get_query_plan() const { return _plan; }

bool SQLQueryOperator::parse_tree_cache_hit() const { return _parse_tree_cache_hit; }

bool SQLQueryOperator::query_plan_cache_hit() const { return _query_plan_cache_hit; }

std::shared_ptr<const Table> SQLQueryOperator::_on_execute(std::shared_ptr<TransactionContext> context) {
  // Compile the query.
  compile_query(_query);

  // Schedule all tasks in query plan.
  if (_schedule_plan) {
    // Add the result task to the end of the query plan.
    std::vector<std::shared_ptr<OperatorTask>> tasks = _plan.tasks();
    if (tasks.size() > 0) {
      _result_op->set_input_operator(tasks.back()->get_operator());
      tasks.back()->set_as_predecessor_of(_result_task);
    }
    tasks.push_back(_result_task);

    for (const auto& task : tasks) {
      task->schedule();
    }
  }

  return nullptr;
}

void SQLQueryOperator::compile_query(const std::string& query) {
  // Check the query plan cache.
  optional<SQLQueryPlan> cached_plan = _query_plan_cache.try_get(_query);
  if (cached_plan) {
    _query_plan_cache_hit = true;
    _plan = (*cached_plan).recreate();
    return;
  }

  // parse the query.
  std::shared_ptr<SQLParserResult> parse_result = parse_query(_query);

  // Populates the query plan in _plan.
  compile_parse_result(parse_result);

  // Cache the plan.
  _query_plan_cache.set(_query, _plan);
}

std::shared_ptr<SQLParserResult> SQLQueryOperator::parse_query(const std::string& query) {
  // Check parse tree cache.
  optional<std::shared_ptr<SQLParserResult>> cached_result = _parse_tree_cache.try_get(_query);
  if (cached_result) {
    _parse_tree_cache_hit = true;
    return *cached_result;
  }

  _parse_tree_cache_hit = false;

  // Parse the query into our result object.
  std::shared_ptr<SQLParserResult> result = std::make_shared<SQLParserResult>();
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
  std::shared_ptr<SQLQueryOperator> op = std::make_shared<SQLQueryOperator>(prepare_stmt.query, false);
  op->execute();

  // Get the plan and cache it.
  SQLQueryPlan plan = op->get_query_plan();
  _prepared_stmts.set(prepare_stmt.name, plan);
}

// Tries to fetch the referenced prepared statement and retrieve its cached data.
void SQLQueryOperator::execute_prepared_statement(const ExecuteStatement& execute_stmt) {
  optional<SQLQueryPlan> plan_template = _prepared_stmts.try_get(execute_stmt.name);
  if (!plan_template) {
    throw std::runtime_error("Requested prepared statement does not exist!");
  }

  // Get list of arguments from EXECUTE statement.
  std::vector<AllParameterVariant> arguments;
  if (execute_stmt.parameters != nullptr) {
    for (const hsql::Expr* expr : *execute_stmt.parameters) {
      arguments.push_back(SQLToASTTranslator::translate_hsql_operand(*expr));
    }
  }

  DebugAssert(arguments.size() == (*plan_template).num_parameters(),
              "Number of arguments in execute statement does not match number of parameters in prepared statement.");

  const SQLQueryPlan plan = (*plan_template).recreate(arguments);
  _plan.append_plan(plan);
}

// Translate the statement and append the result plan
// to the current total query plan (in member _plan).
void SQLQueryOperator::plan_statement(const SQLStatement& stmt) {
  auto result_node = SQLToASTTranslator::get().translate_statement(stmt);
  auto result_operator = ASTToOperatorTranslator::get().translate_node(result_node);

  SQLQueryPlan query_plan;
  query_plan.add_tree_by_root(result_operator);

  _plan.append_plan(query_plan);
}

// Compiles the given parse result into an operator plan.
void SQLQueryOperator::compile_parse_result(std::shared_ptr<SQLParserResult> result) {
  _plan.set_num_parameters(result->parameters().size());

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

std::shared_ptr<AbstractOperator> SQLQueryOperator::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<SQLQueryOperator>(_query, _schedule_plan);
}

// Static.
SQLQueryCache<std::shared_ptr<hsql::SQLParserResult>>& SQLQueryOperator::get_parse_tree_cache() {
  return _parse_tree_cache;
}

// Static.
SQLQueryCache<SQLQueryPlan>& SQLQueryOperator::get_query_plan_cache() { return _query_plan_cache; }

// Static.
SQLQueryCache<SQLQueryPlan>& SQLQueryOperator::get_prepared_statement_cache() { return _prepared_stmts; }

}  // namespace opossum
