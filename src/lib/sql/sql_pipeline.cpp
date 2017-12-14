#include "sql_pipeline.hpp"

#include <boost/algorithm/string.hpp>

#include <iomanip>
#include <utility>

#include "SQLParser.h"
#include "concurrency/transaction_manager.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_translator.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::vector<SQLPipeline> SQLPipeline::from_sql_string(const std::string& sql, const bool use_mvcc) {
  // Pass no explicit transaction context so the SQLPipeline can decide whether it should create one or not.
  return _from_sql_string(sql, nullptr, use_mvcc);
}

std::vector<SQLPipeline> SQLPipeline::from_sql_string(const std::string& sql,
                                                      std::shared_ptr<TransactionContext> transaction_context) {
  DebugAssert(transaction_context != nullptr, "Cannot pass nullptr as explicit transaction context.");
  return _from_sql_string(sql, transaction_context, true);
}

std::vector<SQLPipeline> SQLPipeline::_from_sql_string(const std::string& sql,
                                                       std::shared_ptr<TransactionContext> transaction_context,
                                                       bool use_mvcc) {
  hsql::SQLParserResult parse_result;
  try {
    hsql::SQLParser::parse(sql, &parse_result);
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while parsing SQL query:\n  " + std::string(exception.what()));
  }

  if (!parse_result.isValid()) {
    throw std::runtime_error(_create_parse_error_message(sql, parse_result));
  }

  auto pipelines = std::vector<SQLPipeline>();
  pipelines.reserve(parse_result.size());

  for (auto* statement : parse_result.releaseStatements()) {
    auto parsed_statement = std::make_unique<hsql::SQLParserResult>(statement);
    // We know this is always true because the check above would fail otherwise
    parsed_statement->setIsValid(true);

    // Cannot emplace directly because the constructor is private
    SQLPipeline pipeline{std::move(parsed_statement), transaction_context, use_mvcc};
    pipelines.emplace_back(std::move(pipeline));
  }

  return pipelines;
}

const hsql::SQLParserResult& SQLPipeline::get_parsed_sql() { return *(_parsed_sql.get()); }

const std::shared_ptr<AbstractLQPNode>& SQLPipeline::get_unoptimized_logical_plan() {
  if (_unoptimized_logical_plan) {
    return _unoptimized_logical_plan;
  }

  const auto& parsed_sql = get_parsed_sql();
  try {
    const auto lqp_roots = SQLTranslator{_use_mvcc}.translate_parse_result(parsed_sql);
    DebugAssert(lqp_roots.size() == 1, "LQP translation returned an invalid number of LQP roots");
    _unoptimized_logical_plan = lqp_roots.front();
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while compiling query plan:\n  " + std::string(exception.what()));
  }

  return _unoptimized_logical_plan;
}

const std::shared_ptr<AbstractLQPNode>& SQLPipeline::get_optimized_logical_plan() {
  if (_optimized_logical_plan) {
    return _optimized_logical_plan;
  }

  const auto& unoptimized_lqp = get_unoptimized_logical_plan();
  try {
    _optimized_logical_plan = Optimizer::get().optimize(unoptimized_lqp);
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while optimizing query plan:\n  " + std::string(exception.what()));
  }

  // The optimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. optimized_logical_plan holds the original values now.
  // As the unoptimized LQP is only used for visualization, we can afford to recreate it if necessary.
  _unoptimized_logical_plan = nullptr;

  return _optimized_logical_plan;
}

const SQLQueryPlan& SQLPipeline::get_query_plan() {
  if (_query_plan) {
    return *(_query_plan.get());
  }

  const auto& lqp = get_optimized_logical_plan();
  auto plan = std::make_unique<SQLQueryPlan>();

  const auto started = std::chrono::high_resolution_clock::now();

  try {
    plan->add_tree_by_root(LQPTranslator{}.translate_node(lqp));
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while translating query plan:\n  " + std::string(exception.what()));
  }

  if (_use_mvcc) {
    // If we need a transaction context but haven't passed one in, this is the latest point where we can create it
    if (!_transaction_context) _transaction_context = TransactionManager::get().new_transaction_context();
    plan->set_transaction_context(_transaction_context);
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _compile_time_micro_sec = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  _query_plan = std::move(plan);
  return *(_query_plan.get());
}

const std::vector<std::shared_ptr<OperatorTask>>& SQLPipeline::get_tasks() {
  if (!_tasks.empty()) {
    return _tasks;
  }

  const auto& query_plan = get_query_plan();
  DebugAssert(query_plan.tree_roots().size() == 1, "Invalid number of roots in the physical query plan");

  try {
    const auto& root = query_plan.tree_roots().front();
    _tasks = OperatorTask::make_tasks_from_operator(root);
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while creating tasks:\n  " + std::string(exception.what()));
  }

  return _tasks;
}

const std::shared_ptr<const Table>& SQLPipeline::get_result_table() {
  if (_result_table || !_query_has_output) {
    return _result_table;
  }

  const auto& tasks = get_tasks();

  const auto started = std::chrono::high_resolution_clock::now();

  try {
    CurrentScheduler::schedule_and_wait_for_tasks(tasks);
  } catch (const std::exception& exception) {
    if (_use_mvcc) _transaction_context->rollback();
    throw std::runtime_error("Error while executing tasks:\n  " + std::string(exception.what()));
  }

  if (_auto_commit) {
    _transaction_context->commit();
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _execution_time_micro_sec = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  // Get output from the last task
  _result_table = tasks.back()->get_operator()->get_output();
  if (_result_table == nullptr) _query_has_output = false;

  return _result_table;
}

const std::shared_ptr<TransactionContext>& SQLPipeline::transaction_context() const { return _transaction_context; }

std::chrono::microseconds SQLPipeline::compile_time_microseconds() const {
  Assert(_query_plan != nullptr, "Cannot return compile duration without having created the query plan.");
  return _compile_time_micro_sec;
}

std::chrono::microseconds SQLPipeline::execution_time_microseconds() const {
  Assert(_result_table != nullptr || !_query_has_output, "Cannot return execution duration without having executed.");
  return _execution_time_micro_sec;
}

// private constructor
SQLPipeline::SQLPipeline(std::unique_ptr<const hsql::SQLParserResult> parsed_sql,
                         std::shared_ptr<TransactionContext> transaction_context, bool use_mvcc)
    : _parsed_sql(std::move(parsed_sql)),
      _use_mvcc(use_mvcc),
      _auto_commit(_use_mvcc && transaction_context == nullptr) {
  DebugAssert(_parsed_sql->size() == 1, "SQLPipeline should hold exactly one SQL statement");
  // We don't want to create a new context yet, as it should contain all changes of previously created (possibly even in
  // same query) pipelines up to the point of this pipeline's execution.
  if (transaction_context != nullptr) {
    _transaction_context = std::move(transaction_context);
  }
}

std::string SQLPipeline::_create_parse_error_message(const std::string& sql, const hsql::SQLParserResult& result) {
  std::stringstream error_msg;
  error_msg << "SQL query not valid.\n";

#if IS_DEBUG  // Only create nice error message in debug build
  std::vector<std::string> sql_lines;
  boost::algorithm::split(sql_lines, sql, boost::is_any_of("\n"));

  error_msg << "SQL query:\n==========\n";
  const uint32_t error_line = result.errorLine();
  for (auto line_number = 0u; line_number < sql_lines.size(); ++line_number) {
    error_msg << sql_lines[line_number] << '\n';

    // Add indicator to where the error is
    if (line_number == error_line) {
      const uint32_t error_col = result.errorColumn();
      const auto& line = sql_lines[line_number];

      // Keep indentation of tab characters
      auto num_tabs = std::count(line.begin(), line.begin() + error_col, '\t');
      error_msg << std::string(num_tabs, '\t');

      // Use some color to highlight the error
      const auto color_red = "\x1B[31m";
      const auto color_reset = "\x1B[0m";
      error_msg << std::string(error_col - num_tabs, ' ') << color_red << "^=== ERROR HERE!" << color_reset << "\n";
    }
  }
#endif

  error_msg << "=========="
            << "\nError line: " << result.errorLine() << "\nError column: " << result.errorColumn()
            << "\nError message: " << result.errorMsg();

  return error_msg.str();
}

}  // namespace opossum
