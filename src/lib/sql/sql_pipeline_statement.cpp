#include "sql_pipeline_statement.hpp"

#include <boost/algorithm/string.hpp>

#include <iomanip>
#include <utility>

#include "SQLParser.h"
#include "concurrency/transaction_manager.hpp"
#include "create_sql_parser_error_message.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "utils/assert.hpp"
#include "utils/tracing/probes.hpp"

namespace opossum {

SQLPipelineStatement::SQLPipelineStatement(const std::string& sql, std::shared_ptr<hsql::SQLParserResult> parsed_sql,
                                           const UseMvcc use_mvcc,
                                           const std::shared_ptr<TransactionContext>& transaction_context,
                                           const std::shared_ptr<LQPTranslator>& lqp_translator,
                                           const std::shared_ptr<Optimizer>& optimizer,
                                           const CleanupTemporaries cleanup_temporaries)
    : _sql_string(sql),
      _use_mvcc(use_mvcc),
      _auto_commit(_use_mvcc == UseMvcc::Yes && !transaction_context),
      _transaction_context(transaction_context),
      _lqp_translator(lqp_translator),
      _optimizer(optimizer),
      _parsed_sql_statement(std::move(parsed_sql)),
      _metrics(std::make_shared<SQLPipelineStatementMetrics>()),
      _cleanup_temporaries(cleanup_temporaries) {
  Assert(!_parsed_sql_statement || _parsed_sql_statement->size() == 1,
         "SQLPipelineStatement must hold exactly one SQL statement");
  DebugAssert(!_sql_string.empty(), "An SQLPipelineStatement should always contain a SQL statement string for caching");
  DebugAssert(!_transaction_context || transaction_context->phase() == TransactionPhase::Active,
              "The transaction context cannot have been committed already.");
  DebugAssert(!_transaction_context || use_mvcc == UseMvcc::Yes,
              "Transaction context without MVCC enabled makes no sense");
}

const std::string& SQLPipelineStatement::get_sql_string() { return _sql_string; }

const std::shared_ptr<hsql::SQLParserResult>& SQLPipelineStatement::get_parsed_sql_statement() {
  if (_parsed_sql_statement) {
    return _parsed_sql_statement;
  }

  DebugAssert(!_sql_string.empty(), "Cannot parse empty SQL string");

  _parsed_sql_statement = std::make_shared<hsql::SQLParserResult>();

  hsql::SQLParser::parse(_sql_string, _parsed_sql_statement.get());

  AssertInput(_parsed_sql_statement->isValid(), create_sql_parser_error_message(_sql_string, *_parsed_sql_statement));

  Assert(_parsed_sql_statement->size() == 1,
         "SQLPipelineStatement must hold exactly one statement. "
         "Use SQLPipeline when you have multiple statements.");

  return _parsed_sql_statement;
}

const std::shared_ptr<AbstractLQPNode>& SQLPipelineStatement::get_unoptimized_logical_plan() {
  if (_unoptimized_logical_plan) {
    return _unoptimized_logical_plan;
  }

  auto parsed_sql = get_parsed_sql_statement();

  const auto started = std::chrono::high_resolution_clock::now();

  SQLTranslator sql_translator{_use_mvcc};

  std::vector<std::shared_ptr<AbstractLQPNode>> lqp_roots;
  lqp_roots = sql_translator.translate_parser_result(*parsed_sql);

  DebugAssert(lqp_roots.size() == 1, "LQP translation returned no or more than one LQP root for a single statement.");
  _unoptimized_logical_plan = lqp_roots.front();

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->sql_translation_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);

  return _unoptimized_logical_plan;
}

const std::shared_ptr<AbstractLQPNode>& SQLPipelineStatement::get_optimized_logical_plan() {
  if (_optimized_logical_plan) {
    return _optimized_logical_plan;
  }

  // Handle logical query plan if statement has been cached
  if (const auto cached_plan = SQLLogicalPlanCache::get().try_get(_sql_string)) {
    const auto plan = *cached_plan;
    DebugAssert(plan, "Optimized logical query plan retrieved from cache is empty.");
    // MVCC-enabled and MVCC-disabled LQPs will evict each other
    if (lqp_is_validated(plan) == (_use_mvcc == UseMvcc::Yes)) {
      _optimized_logical_plan = plan;
      return _optimized_logical_plan;
    }
  }

  const auto& unoptimized_lqp = get_unoptimized_logical_plan();

  const auto started = std::chrono::high_resolution_clock::now();

  _optimized_logical_plan = _optimizer->optimize(unoptimized_lqp);

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->optimization_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);

  // The optimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. optimized_logical_plan holds the original values now.
  // As the unoptimized LQP is only used for visualization, we can afford to recreate it if necessary.
  _unoptimized_logical_plan = nullptr;

  // Cache newly created plan for the according sql statement
  SQLLogicalPlanCache::get().set(_sql_string, _optimized_logical_plan);

  return _optimized_logical_plan;
}

const std::shared_ptr<AbstractOperator>& SQLPipelineStatement::get_physical_plan() {
  if (_physical_plan) {
    return _physical_plan;
  }

  // If we need a transaction context but haven't passed one in, this is the latest point where we can create it
  if (!_transaction_context && _use_mvcc == UseMvcc::Yes) {
    _transaction_context = TransactionManager::get().new_transaction_context();
  }

  // Stores when the actual compilation started/ended
  auto started = std::chrono::high_resolution_clock::now();
  auto done = started;  // dummy value needed for initialization

  if (const auto cached_physical_plan = SQLPhysicalPlanCache::get().try_get(_sql_string)) {
    if ((*cached_physical_plan)->transaction_context_is_set()) {
      Assert(_use_mvcc == UseMvcc::Yes, "Trying to use MVCC cached query without a transaction context.");
    } else {
      Assert(_use_mvcc == UseMvcc::No, "Trying to use non-MVCC cached query with a transaction context.");
    }

    _physical_plan = (*cached_physical_plan)->deep_copy();
    _metrics->query_plan_cache_hit = true;

  } else {
    // "Normal" mode in which the query plan is created
    const auto& lqp = get_optimized_logical_plan();

    // Reset time to exclude previous pipeline steps
    started = std::chrono::high_resolution_clock::now();
    _physical_plan = _lqp_translator->translate_node(lqp);
  }

  done = std::chrono::high_resolution_clock::now();

  if (_use_mvcc == UseMvcc::Yes) _physical_plan->set_transaction_context_recursively(_transaction_context);

  // Cache newly created plan for the according sql statement (only if not already cached)
  if (!_metrics->query_plan_cache_hit) {
    SQLPhysicalPlanCache::get().set(_sql_string, _physical_plan);
  }

  _metrics->lqp_translation_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);

  return _physical_plan;
}

const std::vector<std::shared_ptr<OperatorTask>>& SQLPipelineStatement::get_tasks() {
  if (!_tasks.empty()) {
    return _tasks;
  }

  _tasks = OperatorTask::make_tasks_from_operator(get_physical_plan(), _cleanup_temporaries);
  return _tasks;
}

const std::shared_ptr<const Table>& SQLPipelineStatement::get_result_table() {
  if (_result_table || !_query_has_output) {
    return _result_table;
  }

  const auto& tasks = get_tasks();

  const auto started = std::chrono::high_resolution_clock::now();

  DTRACE_PROBE3(HYRISE, TASKS_PER_STATEMENT, reinterpret_cast<uintptr_t>(&tasks), _sql_string.c_str(),
                reinterpret_cast<uintptr_t>(this));
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  if (_auto_commit) {
    _transaction_context->commit();
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->plan_execution_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);

  // Get output from the last task
  _result_table = tasks.back()->get_operator()->get_output();
  if (_result_table == nullptr) _query_has_output = false;

  DTRACE_PROBE8(HYRISE, SUMMARY, _sql_string.c_str(), _metrics->sql_translation_duration.count(),
                _metrics->optimization_duration.count(), _metrics->lqp_translation_duration.count(),
                _metrics->plan_execution_duration.count(), _metrics->query_plan_cache_hit, get_tasks().size(),
                reinterpret_cast<uintptr_t>(this));
  return _result_table;
}

const std::shared_ptr<TransactionContext>& SQLPipelineStatement::transaction_context() const {
  return _transaction_context;
}

const std::shared_ptr<SQLPipelineStatementMetrics>& SQLPipelineStatement::metrics() const { return _metrics; }
}  // namespace opossum
