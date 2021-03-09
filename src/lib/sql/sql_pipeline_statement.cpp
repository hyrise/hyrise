#include "sql_pipeline_statement.hpp"

#include <fstream>
#include <iomanip>
#include <utility>

#include <boost/algorithm/string.hpp>

#include "SQLParser.h"
#include "create_sql_parser_error_message.hpp"
#include "expression/value_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/export.hpp"
#include "operators/import.hpp"
#include "operators/maintenance/create_prepared_plan.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/maintenance/create_view.hpp"
#include "operators/maintenance/drop_table.hpp"
#include "operators/maintenance/drop_view.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/job_task.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "utils/assert.hpp"
#include "utils/tracing/probes.hpp"

namespace opossum {

SQLPipelineStatement::SQLPipelineStatement(const std::string& sql, std::shared_ptr<hsql::SQLParserResult> parsed_sql,
                                           const UseMvcc use_mvcc, const std::shared_ptr<Optimizer>& optimizer,
                                           const std::shared_ptr<SQLPhysicalPlanCache>& init_pqp_cache,
                                           const std::shared_ptr<SQLLogicalPlanCache>& init_lqp_cache)
    : pqp_cache(init_pqp_cache),
      lqp_cache(init_lqp_cache),
      _sql_string(sql),
      _use_mvcc(use_mvcc),
      _optimizer(optimizer),
      _parsed_sql_statement(std::move(parsed_sql)),
      _metrics(std::make_shared<SQLPipelineStatementMetrics>()) {
  Assert(!_parsed_sql_statement || _parsed_sql_statement->size() == 1,
         "SQLPipelineStatement must hold exactly one SQL statement");
  DebugAssert(!_sql_string.empty(), "An SQLPipelineStatement should always contain a SQL statement string for caching");
}

void SQLPipelineStatement::set_transaction_context(const std::shared_ptr<TransactionContext>& transaction_context) {
  Assert(!_transaction_context, "SQLPipelineStatement already has a transaction context");
  Assert(!transaction_context || !transaction_context->is_auto_commit(),
         "Auto-commit transaction contexts should be created by the SQLPipelineStatement itself");
  Assert(_use_mvcc == UseMvcc::Yes || !transaction_context,
         "Can only set transaction context for MVCC-enabled statements");
  _transaction_context = transaction_context;
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

  auto translation_result = sql_translator.translate_parser_result(*parsed_sql);
  std::vector<std::shared_ptr<AbstractLQPNode>> lqp_roots = translation_result.lqp_nodes;
  _translation_info = translation_result.translation_info;

  DebugAssert(lqp_roots.size() == 1, "LQP translation returned no or more than one LQP root for a single statement.");

  _unoptimized_logical_plan = lqp_roots.front();

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->sql_translation_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);

  return _unoptimized_logical_plan;
}

const SQLTranslationInfo& SQLPipelineStatement::get_sql_translation_info() {
  // Make sure that the SQLTranslator was invoked
  (void)get_unoptimized_logical_plan();

  return _translation_info;
}

const std::shared_ptr<AbstractLQPNode>& SQLPipelineStatement::get_optimized_logical_plan() {
  if (_optimized_logical_plan) {
    return _optimized_logical_plan;
  }

  // Handle logical query plan if statement has been cached
  if (lqp_cache) {
    if (const auto cached_plan = lqp_cache->try_get(_sql_string)) {
      const auto plan = *cached_plan;
      DebugAssert(plan, "Optimized logical query plan retrieved from cache is empty.");
      // MVCC-enabled and MVCC-disabled LQPs will evict each other
      if (lqp_is_validated(plan) == (_use_mvcc == UseMvcc::Yes)) {
        // Copy the LQP for reuse as the LQPTranslator might modify mutable fields (e.g., cached output_expressions)
        // and concurrent translations might conflict.
        _optimized_logical_plan = plan->deep_copy();
        return _optimized_logical_plan;
      }
    }
  }

  auto unoptimized_lqp = get_unoptimized_logical_plan();

  const auto started = std::chrono::high_resolution_clock::now();

  // The optimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. optimized_logical_plan holds the original values now.
  // As the unoptimized LQP is only used for visualization, we can afford to recreate it if necessary.
  _unoptimized_logical_plan = nullptr;

  auto optimizer_rule_durations = std::make_shared<std::vector<OptimizerRuleMetrics>>();

  _optimized_logical_plan = _optimizer->optimize(std::move(unoptimized_lqp), optimizer_rule_durations);

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->optimization_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);
  _metrics->optimizer_rule_durations = *optimizer_rule_durations;

  // Cache newly created plan for the according sql statement
  if (lqp_cache && _translation_info.cacheable) {
    lqp_cache->set(_sql_string, _optimized_logical_plan);
  }

  return _optimized_logical_plan;
}

const std::shared_ptr<AbstractOperator>& SQLPipelineStatement::get_physical_plan() {
  if (_physical_plan) {
    return _physical_plan;
  }

  // If we need a transaction context but haven't passed one in, this is the last point where we can create it
  if (!_transaction_context && _use_mvcc == UseMvcc::Yes) {
    _transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  }

  // Stores when the actual compilation started/ended
  auto started = std::chrono::high_resolution_clock::now();
  auto done = started;  // dummy value needed for initialization

  // Try to retrieve the PQP from cache
  if (pqp_cache) {
    if (const auto cached_physical_plan = pqp_cache->try_get(_sql_string)) {
      if ((*cached_physical_plan)->transaction_context_is_set()) {
        Assert(_use_mvcc == UseMvcc::Yes, "Trying to use MVCC cached query without a transaction context.");
      } else {
        Assert(_use_mvcc == UseMvcc::No, "Trying to use non-MVCC cached query with a transaction context.");
      }

      _physical_plan = (*cached_physical_plan)->deep_copy();
      _metrics->query_plan_cache_hit = true;
    }
  }

  if (!_physical_plan) {
    // "Normal" path in which the query plan is created instead of begin retrieved from cache
    const auto& lqp = get_optimized_logical_plan();

    // Reset time to exclude previous pipeline steps
    started = std::chrono::high_resolution_clock::now();
    _physical_plan = LQPTranslator{}.translate_node(lqp);
  }

  done = std::chrono::high_resolution_clock::now();

  if (_use_mvcc == UseMvcc::Yes) _physical_plan->set_transaction_context_recursively(_transaction_context);

  // Cache newly created plan for the according sql statement (only if not already cached)
  if (pqp_cache && !_metrics->query_plan_cache_hit && _translation_info.cacheable) {
    pqp_cache->set(_sql_string, _physical_plan);
  }

  _metrics->lqp_translation_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);

  return _physical_plan;
}

const std::vector<std::shared_ptr<AbstractTask>>& SQLPipelineStatement::get_tasks() {
  if (!_tasks.empty()) {
    return _tasks;
  }

  if (_is_transaction_statement()) {
    _tasks = _get_transaction_tasks();
  } else {
    _precheck_ddl_operators(get_physical_plan());
    auto operator_tasks = OperatorTask::make_tasks_from_operator(get_physical_plan());
    _tasks = std::vector<std::shared_ptr<AbstractTask>>(operator_tasks.cbegin(), operator_tasks.cend());
  }
  return _tasks;
}

std::vector<std::shared_ptr<AbstractTask>> SQLPipelineStatement::_get_transaction_tasks() {
  const auto& sql_statement = get_parsed_sql_statement();
  const std::vector<hsql::SQLStatement*>& statements = sql_statement->getStatements();
  const auto& transaction_statement = static_cast<const hsql::TransactionStatement&>(*statements.front());

  switch (transaction_statement.command) {
    case hsql::kBeginTransaction:
      AssertInput(!_transaction_context || _transaction_context->is_auto_commit(),
                  "Cannot begin transaction inside an active transaction.");
      return {std::make_shared<JobTask>([this] {
        _transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
      })};
    case hsql::kCommitTransaction:
      AssertInput(_transaction_context && !_transaction_context->is_auto_commit(),
                  "Cannot commit since there is no active transaction.");
      return {std::make_shared<JobTask>([this] { _transaction_context->commit(); })};
    case hsql::kRollbackTransaction:
      AssertInput(_transaction_context && !_transaction_context->is_auto_commit(),
                  "Cannot rollback since there is no active transaction.");
      return {std::make_shared<JobTask>([this] { _transaction_context->rollback(RollbackReason::User); })};
    default:
      Fail("Unexpected transaction command!");
  }
}

std::pair<SQLPipelineStatus, const std::shared_ptr<const Table>&> SQLPipelineStatement::get_result_table() {
  // Returns true if a transaction was set and that transaction was rolled back.
  const auto has_failed = [&]() {
    if (_transaction_context) {
      DebugAssert(_transaction_context->phase() == TransactionPhase::Active ||
                      _transaction_context->phase() == TransactionPhase::RolledBackByUser ||
                      _transaction_context->phase() == TransactionPhase::RolledBackAfterConflict ||
                      _transaction_context->phase() == TransactionPhase::Committed,
                  "Transaction found in unexpected state");
      return _transaction_context->phase() == TransactionPhase::RolledBackAfterConflict;
    }
    return false;
  };

  if (has_failed() && !_is_transaction_statement()) {
    return {SQLPipelineStatus::Failure, _result_table};
  }

  if (_result_table || !_query_has_output) {
    return {SQLPipelineStatus::Success, _result_table};
  }

  const auto& tasks = get_tasks();

  const auto started = std::chrono::high_resolution_clock::now();

  DTRACE_PROBE3(HYRISE, TASKS_PER_STATEMENT, reinterpret_cast<uintptr_t>(&tasks), _sql_string.c_str(),
                reinterpret_cast<uintptr_t>(this));

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  if (has_failed()) {
    return {SQLPipelineStatus::Failure, _result_table};
  }

  if (_use_mvcc == UseMvcc::Yes && _transaction_context->is_auto_commit()) {
    _transaction_context->commit();
  }

  if (_transaction_context) {
    Assert(_transaction_context->phase() == TransactionPhase::Active ||
               _transaction_context->phase() == TransactionPhase::Committed ||
               _transaction_context->phase() == TransactionPhase::RolledBackByUser,
           "Transaction should either be still active or have been auto-committed by now");
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->plan_execution_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(done - started);

  // Get output from the last task if the task was an actual operator and not a transaction statement
  if (!_is_transaction_statement()) {
    const auto& last_executed_operator = static_cast<const OperatorTask&>(*tasks.back()).get_operator();
    _result_table = last_executed_operator->get_output();
    last_executed_operator->clear_output();
  }

  if (!_result_table) _query_has_output = false;

  DTRACE_PROBE8(HYRISE, SUMMARY, _sql_string.c_str(), _metrics->sql_translation_duration.count(),
                _metrics->optimization_duration.count(), _metrics->lqp_translation_duration.count(),
                _metrics->plan_execution_duration.count(), _metrics->query_plan_cache_hit, get_tasks().size(),
                reinterpret_cast<uintptr_t>(this));

  return {SQLPipelineStatus::Success, _result_table};
}

const std::shared_ptr<TransactionContext>& SQLPipelineStatement::transaction_context() const {
  return _transaction_context;
}

const std::shared_ptr<SQLPipelineStatementMetrics>& SQLPipelineStatement::metrics() const { return _metrics; }

void SQLPipelineStatement::_precheck_ddl_operators(const std::shared_ptr<AbstractOperator>& pqp) {
  const auto& storage_manager = Hyrise::get().storage_manager;

  /**
   * Only look at the root operator, because as of now DDL operators are always at the root.
   */

  switch (pqp->type()) {
    case OperatorType::CreatePreparedPlan: {
      const auto create_prepared_plan = std::dynamic_pointer_cast<CreatePreparedPlan>(pqp);
      AssertInput(!storage_manager.has_prepared_plan(create_prepared_plan->prepared_plan_name()),
                  "Prepared Plan '" + create_prepared_plan->prepared_plan_name() + "' already exists.");
      break;
    }
    case OperatorType::CreateTable: {
      const auto create_table = std::dynamic_pointer_cast<CreateTable>(pqp);
      AssertInput(create_table->if_not_exists || !storage_manager.has_table(create_table->table_name),
                  "Table '" + create_table->table_name + "' already exists.");
      break;
    }
    case OperatorType::CreateView: {
      const auto create_view = std::dynamic_pointer_cast<CreateView>(pqp);
      AssertInput(create_view->if_not_exists() || !storage_manager.has_view(create_view->view_name()),
                  "View '" + create_view->view_name() + "' already exists.");
      break;
    }
    case OperatorType::DropTable: {
      const auto drop_table = std::dynamic_pointer_cast<DropTable>(pqp);
      AssertInput(drop_table->if_exists || storage_manager.has_table(drop_table->table_name),
                  "There is no table '" + drop_table->table_name + "'.");
      break;
    }
    case OperatorType::DropView: {
      const auto drop_view = std::dynamic_pointer_cast<DropView>(pqp);
      AssertInput(drop_view->if_exists || storage_manager.has_view(drop_view->view_name),
                  "There is no view '" + drop_view->view_name + "'.");
      break;
    }
    case OperatorType::Import: {
      const auto import = std::dynamic_pointer_cast<Import>(pqp);
      std::ifstream file(import->filename);
      AssertInput(file.good(), "There is no file '" + import->filename + "'.");
      break;
    }
    default:
      break;
  }
}

bool SQLPipelineStatement::_is_transaction_statement() {
  return get_parsed_sql_statement()->getStatements().front()->isType(hsql::kStmtTransaction);
}

}  // namespace opossum
