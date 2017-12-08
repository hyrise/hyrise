#include <utility>

#include "SQLParser.h"
#include "concurrency/transaction_manager.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql_pipeline.hpp"
#include "sql_translator.hpp"
#include "utils/assert.hpp"

namespace opossum {

SQLPipeline::SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context)
    : _sql_string(sql), _use_mvcc(true), _auto_commit(false), _transaction_context(std::move(transaction_context)) {
  DebugAssert(_transaction_context != nullptr, "Cannot pass nullptr as explicit transaction context.");
}

SQLPipeline::SQLPipeline(const std::string& sql, const bool use_mvcc)
    : _sql_string(sql), _use_mvcc(use_mvcc), _auto_commit(_use_mvcc) {
  if (_use_mvcc) {
    // We want to use MVCC but didn't pass an explicit context
    _transaction_context = TransactionManager::get().new_transaction_context();
  } else {
    // We don't want to use MVCC
    _transaction_context = nullptr;
  }
}

const hsql::SQLParserResult& SQLPipeline::get_parsed_sql() {
  if (_parsed_sql) {
    // Returned cached result
    return *(_parsed_sql.get());
  }

  auto parse_result = std::make_unique<hsql::SQLParserResult>();

  const auto started = std::chrono::high_resolution_clock::now();
  try {
    hsql::SQLParser::parse(_sql_string, parse_result.get());
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while parsing SQL query:\n  " + std::string(exception.what()));
  }

  if (!parse_result->isValid()) {
    throw std::logic_error("SQL query not valid.");
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _parse_time_sec = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  _parsed_sql = std::move(parse_result);
  return *(_parsed_sql.get());
}

const std::vector<std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_unoptimized_logical_plans() {
  if (!_unoptimized_logical_plans.empty()) {
    return _unoptimized_logical_plans;
  }

  const auto& parsed_sql = get_parsed_sql();
  try {
    _unoptimized_logical_plans = SQLTranslator{_use_mvcc}.translate_parse_result(parsed_sql);
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while compiling query plan:\n  " + std::string(exception.what()));
  }

  return _unoptimized_logical_plans;
}

const std::vector<std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_optimized_logical_plans() {
  if (!_optimized_logical_plans.empty()) {
    return _optimized_logical_plans;
  }

  const auto& unoptimized_lqp = get_unoptimized_logical_plans();
  _optimized_logical_plans.reserve(unoptimized_lqp.size());

  try {
    for (const auto& node : unoptimized_lqp) {
      _optimized_logical_plans.push_back(Optimizer::get().optimize(node));
    }
  } catch (const std::exception& exception) {
    // Don't keep bad values
    _optimized_logical_plans.clear();
    throw std::runtime_error("Error while optimizing query plan:\n  " + std::string(exception.what()));
  }

  // The optimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. optimized_logical_plans then holds hold the original values now.
  // As the unoptimized LQP is only used for visualization, we can afford to recreate it if necessary.
  _unoptimized_logical_plans.clear();

  return _optimized_logical_plans;
}

const SQLQueryPlan& SQLPipeline::get_query_plan() {
  if (_query_plan) {
    return *(_query_plan.get());
  }

  const auto& lqp_roots = get_optimized_logical_plans();
  auto plan = std::make_unique<SQLQueryPlan>();

  const auto started = std::chrono::high_resolution_clock::now();

  try {
    for (const auto& node : lqp_roots) {
      auto op = LQPTranslator{}.translate_node(node);
      plan->add_tree_by_root(op);
    }
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while translating query plan:\n  " + std::string(exception.what()));
  }

  if (_use_mvcc) {
    plan->set_transaction_context(_transaction_context);
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _compile_time_sec = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  _query_plan = std::move(plan);
  return *(_query_plan.get());
}

const std::vector<std::shared_ptr<OperatorTask>>& SQLPipeline::get_tasks() {
  if (!_op_tasks.empty()) {
    return _op_tasks;
  }

  const auto& query_plan = get_query_plan();
  try {
    _op_tasks = query_plan.create_tasks();
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while creating tasks:\n  " + std::string(exception.what()));
  }

  return _op_tasks;
}

const std::shared_ptr<const Table>& SQLPipeline::get_result_table() {
  if (_result_table || !_query_has_output) {
    return _result_table;
  }

  const auto& op_tasks = get_tasks();

  const auto started = std::chrono::high_resolution_clock::now();

  try {
    CurrentScheduler::schedule_and_wait_for_tasks(op_tasks);
  } catch (const std::exception& exception) {
    if (_use_mvcc) _transaction_context->rollback();
    throw std::runtime_error("Error while executing tasks:\n  " + std::string(exception.what()));
  }

  if (_auto_commit) {
    _transaction_context->commit();
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _execution_time_sec = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  _result_table = op_tasks.back()->get_operator()->get_output();
  if (_result_table == nullptr) _query_has_output = false;

  return _result_table;
}

const std::shared_ptr<TransactionContext>& SQLPipeline::transaction_context() { return _transaction_context; }

std::chrono::microseconds SQLPipeline::parse_time_microseconds() {
  Assert(_parsed_sql != nullptr, "Cannot return parse duration without having parsed.");
  return _parse_time_sec;
}

std::chrono::microseconds SQLPipeline::compile_time_microseconds() {
  Assert(_query_plan != nullptr, "Cannot return compile duration without having created the query plan.");
  return _compile_time_sec;
}

std::chrono::microseconds SQLPipeline::execution_time_microseconds() {
  Assert(_result_table != nullptr || !_query_has_output, "Cannot return execution duration without having executed.");
  return _execution_time_sec;
}

}  // namespace opossum
