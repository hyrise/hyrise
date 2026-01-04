#include "sql_pipeline_statement.hpp"

#include <chrono>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "SQLParser.h"
#include "SQLParserResult.h"

#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "create_sql_parser_error_message.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/import.hpp"
#include "operators/maintenance/create_prepared_plan.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/maintenance/create_view.hpp"
#include "operators/maintenance/drop_table.hpp"
#include "operators/maintenance/drop_view.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/optimization_context.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "sql/sql_plan_cache.hpp"
#include "sql/sql_translator.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

SQLPipelineStatement::SQLPipelineStatement(const std::string& sql, std::shared_ptr<hsql::SQLParserResult> parsed_sql,
                                           const UseMvcc use_mvcc, const std::shared_ptr<Optimizer>& optimizer,
                                           const std::shared_ptr<SQLPhysicalPlanCache>& init_pqp_cache,
                                           const std::shared_ptr<SQLLogicalPlanCache>& init_lqp_cache,
                                           const std::shared_ptr<Optimizer>& data_dependency_optimizer)
    : pqp_cache(init_pqp_cache),
      lqp_cache(init_lqp_cache),
      _sql_string(sql),
      _use_mvcc(use_mvcc),
      _optimizer(optimizer),
      _data_dependency_optimizer(data_dependency_optimizer),
      _parsed_sql_statement(std::move(parsed_sql)),
      _metrics(std::make_shared<SQLPipelineStatementMetrics>()) {
  Assert(!_parsed_sql_statement || _parsed_sql_statement->size() == 1,
         "SQLPipelineStatement must hold exactly one SQL statement");
  DebugAssert(!_sql_string.empty(),
              "An SQLPipelineStatement should always contain a SQL statement string for caching.");
}

void SQLPipelineStatement::set_transaction_context(const std::shared_ptr<TransactionContext>& transaction_context) {
  Assert(!_transaction_context, "SQLPipelineStatement already has a transaction context");
  Assert(!transaction_context || !transaction_context->is_auto_commit(),
         "Auto-commit transaction contexts should be created by the SQLPipelineStatement itself");
  Assert(_use_mvcc == UseMvcc::Yes || !transaction_context,
         "Can only set transaction context for MVCC-enabled statements");
  _transaction_context = transaction_context;
}

const std::string& SQLPipelineStatement::get_sql_string() {
  return _sql_string;
}

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

  const auto started = std::chrono::steady_clock::now();

  SQLTranslator sql_translator{_use_mvcc};

  auto translation_result = sql_translator.translate_parser_result(*parsed_sql);
  const auto lqp_roots = translation_result.lqp_nodes;
  _translation_info = translation_result.translation_info;

  DebugAssert(lqp_roots.size() == 1, "LQP translation returned no or more than one LQP root for a single statement.");

  _unoptimized_logical_plan = lqp_roots.front();

  const auto done = std::chrono::steady_clock::now();
  _metrics->sql_translation_duration = done - started;

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
      const auto& plan = *cached_plan;
      DebugAssert(plan, "Optimized logical query plan retrieved from cache is empty.");
      // MVCC-enabled and MVCC-disabled LQPs will evict each other
      if (lqp_is_validated(plan) == (_use_mvcc == UseMvcc::Yes)) {
        // Copy the LQP for reuse as the LQPTranslator might modify mutable fields (e.g., cached output_expressions)
        // and concurrent translations might conflict.
        // Note that the plan we have received here was cached, so it is obviously cacheable. This is why a missing
        // optimization context leads to a cacheable PQP.
        _optimized_logical_plan = plan->deep_copy();
        _optimization_context = nullptr;
        return _optimized_logical_plan;
      }
    }
  }

  auto unoptimized_lqp = get_unoptimized_logical_plan();

  const auto started = std::chrono::steady_clock::now();

  // The optimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. optimized_logical_plan holds the original values now.
  // As the unoptimized LQP is only used for visualization, we can afford to recreate it if necessary.
  _unoptimized_logical_plan = nullptr;

  auto optimizer_rule_durations = std::make_shared<std::vector<OptimizerRuleMetrics>>();

  std::tie(_optimized_logical_plan, _optimization_context) =
      _optimizer->optimize_with_context(std::move(unoptimized_lqp), optimizer_rule_durations);
  std::cout << "first optimization done" << '\n';

  visit_lqp(_optimized_logical_plan, [&](const std::shared_ptr<const AbstractLQPNode>& lqp) {
    _optimizer->estimate_cardinality(const_cast<std::shared_ptr<const AbstractLQPNode>&>(lqp));
    return LQPVisitation::VisitInputs;
  });
  std::cout << "estimated with same cardinality estimator" << '\n';
  const auto done = std::chrono::steady_clock::now();
  _metrics->optimization_duration = done - started;
  _metrics->optimizer_rule_durations = *optimizer_rule_durations;

  auto data_dependency_cardinality_estimator = CardinalityEstimator::new_instance_with_optimizations();
  auto default_cardinality_estimator = CardinalityEstimator::new_instance();

  // _metrics->data_dependencies_estimated_cardinality = data_dependency_cardinality_estimator->estimate_cardinality(
  //     _optimized_logical_plan);
  // _metrics->estimated_cardinality = default_cardinality_estimator->estimate_cardinality(
  //     _optimized_logical_plan);

  // std::cout << "Estimated cardinality: " << _metrics->estimated_cardinality << std::endl;
  // std::cout << "Data dependencies estimated cardinality: " << _metrics->data_dependencies_estimated_cardinality
  //           << std::endl;

  // Cache newly created plan for the according sql statement
  if (lqp_cache && _translation_info.cacheable && _optimization_context->is_cacheable()) {
    lqp_cache->set(_sql_string, _optimized_logical_plan);
  }
  std::cout << "optimized_logical_plan" << "\n";
  return _optimized_logical_plan;
}

const std::shared_ptr<AbstractOperator>& SQLPipelineStatement::get_physical_plan() {
  if (_physical_plan) {
    return _physical_plan;
  }

  // If we need a transaction context but have not passed one in, this is the last point where we can create it.
  if (!_transaction_context && _use_mvcc == UseMvcc::Yes) {
    _transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  }

  // Stores when the actual compilation started/ended.
  auto started = std::chrono::steady_clock::now();
  auto done = started;  // dummy value needed for initialization

  // Try to retrieve the PQP from cache.
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
    // "Normal" path in which the query plan is created instead of begin retrieved from cache.
    const auto& lqp = get_optimized_logical_plan();
    // Reset time to exclude previous pipeline steps
    started = std::chrono::steady_clock::now();
    _physical_plan = LQPTranslator{}.translate_node(lqp);
  }

  done = std::chrono::steady_clock::now();

  if (_use_mvcc == UseMvcc::Yes) {
    _physical_plan->set_transaction_context_recursively(_transaction_context);
  }

  // Cache the newly created PQP for the according SQL statement (only if not already cached). If the LQP was cached
  // (`_optimization_context` is set to `nullptr`), we can also safely cache the PQP.
  if (pqp_cache && !_metrics->query_plan_cache_hit && _translation_info.cacheable &&
      (!_optimization_context || _optimization_context->is_cacheable())) {
    pqp_cache->set(_sql_string, _physical_plan);
  }

  _metrics->lqp_translation_duration = done - started;

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
    std::tie(_tasks, _root_operator_task) = OperatorTask::make_tasks_from_operator(get_physical_plan());
  }
  return _tasks;
}

std::vector<std::shared_ptr<AbstractTask>> SQLPipelineStatement::_get_transaction_tasks() {
  const auto& sql_statement = get_parsed_sql_statement();
  const auto& transaction_statement =
      static_cast<const hsql::TransactionStatement&>(*sql_statement->getStatements().front());

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
      return {std::make_shared<JobTask>([this] {
        _transaction_context->commit();
      })};
    case hsql::kRollbackTransaction:
      AssertInput(_transaction_context && !_transaction_context->is_auto_commit(),
                  "Cannot rollback since there is no active transaction.");
      return {std::make_shared<JobTask>([this] {
        _transaction_context->rollback(RollbackReason::User);
      })};
    default:
      Fail("Unexpected transaction command!");
  }
}

std::pair<SQLPipelineStatus, const std::shared_ptr<const Table>&> SQLPipelineStatement::get_result_table() {
  auto data_dependency_cardinality_estimator = CardinalityEstimator::new_instance_with_optimizations();
  // auto default_cardinality_estimator = CardinalityEstimator::new_instance();
  auto* default_cardinality_estimator = &_optimizer->cardinality_estimator();

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

  const auto started = std::chrono::steady_clock::now();

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

  const auto done = std::chrono::steady_clock::now();
  _metrics->plan_execution_duration = done - started;

  // Get result table, if it was not a transaction statement
  if (!_is_transaction_statement()) {
    // After execution, the root operator should be the only operator in OperatorState::ExecutedAndAvailable. All
    // other operators should be in OperatorState::ExecutedAndCleared.
    Assert(_root_operator_task && _root_operator_task->get_operator()->state() == OperatorState::ExecutedAndAvailable,
           "Expected root operator to be in OperatorState::ExecutedAndAvailable.");
    if constexpr (HYRISE_DEBUG) {
      for (const auto& task : tasks) {
        const auto operator_task = std::static_pointer_cast<OperatorTask>(task);
        if (operator_task == _root_operator_task) {
          continue;
        }
        Assert(operator_task->get_operator()->state() == OperatorState::ExecutedAndCleared,
               "Expected non-root operator to be in OperatorState::ExecutedAndCleared.");
      }
    }
    _result_table = _root_operator_task->get_operator()->get_output();

    default_cardinality_estimator->estimate_cardinality(_root_operator_task->get_operator()->lqp_node);

    visit_pqp(_root_operator_task->get_operator(), [&](const std::shared_ptr<AbstractOperator>& pqp) {
      if (pqp->type() == OperatorType::Validate) {
        _metrics->operator_cardinality_metrics.push_back(
            {.operator_type = pqp->type(),
             .operator_hash = pqp->lqp_node->hash(),
             .left_input_hash = pqp->left_input() ? pqp->left_input()->lqp_node->hash() : 0,
             .right_input_hash = pqp->right_input() ? pqp->right_input()->lqp_node->hash() : 0});
      }
      if (pqp->performance_data->has_output && pqp->type() != OperatorType::Validate) {
        auto true_cardinality = static_cast<double>(pqp->performance_data->output_row_count);
        auto data_dependency_estimation = data_dependency_cardinality_estimator->estimate_cardinality(pqp->lqp_node);
        auto default_estimation =
            default_cardinality_estimator->estimate_statistics(pqp->lqp_node).table_statistics->row_count;
        // auto test = default_cardinality_estimator->estimate_cardinality(pqp->lqp_node);

        // estimate_cardinality(pqp->lqp_node);

        _metrics->operator_cardinality_metrics.push_back(
            {.true_cardinality = true_cardinality,
             .estimated_cardinality = default_estimation,
             .data_dependencies_estimated_cardinality = data_dependency_estimation,
             .operator_type = pqp->type(),
             .operator_hash = pqp->lqp_node->hash(),
             .left_input_hash = pqp->left_input() ? pqp->left_input()->lqp_node->hash() : 0,
             .right_input_hash = pqp->right_input() ? pqp->right_input()->lqp_node->hash() : 0});
      }
      return PQPVisitation::VisitInputs;
    });

    std::map<DataType, size_t> predicate_column_datatypes_count;
    std::map<DataType, size_t> join_column_datatype_count;

    visit_pqp(_root_operator_task->get_operator(), [&](const std::shared_ptr<AbstractOperator>& pqp) {
      if (pqp->performance_data->has_output && pqp->type() != OperatorType::Validate) {
        if (pqp->lqp_node->type == LQPNodeType::Predicate) {
          const auto& predicate_node = static_cast<const PredicateNode&>(*pqp->lqp_node);
          const auto predicate = predicate_node.predicate();
          const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate, predicate_node);
          if (!operator_scan_predicates.has_value()) {
            return PQPVisitation::VisitInputs;
          }

          auto columnid = operator_scan_predicates->front().column_id;

          // auto table_statistics = default_cardinality_estimator->estimate_statistics(pqp->lqp_node);
          auto estimation_state = default_cardinality_estimator->estimate_statistics(pqp->lqp_node->left_input());
          auto left_table_statistics = estimation_state.table_statistics;
          if (columnid > left_table_statistics->column_statistics.size()) {
            std::cout << "ColumnId: " << columnid << " is out of bounds for table statistics with size: "
                      << left_table_statistics->column_statistics.size() << '\n';
            return PQPVisitation::VisitInputs;
          }

          // std::cout << "ColumnId: " << columnid << " Table statistics size: " << table_statistics->column_statistics.size()  << std::endl;

          // std::cout << "Node Statistics " << *table_statistics << std::endl;

          // std::cout << "Left Node Statistics " << *left_table_statistics << std::endl;

          auto predicate_datatype = left_table_statistics->column_data_type(columnid);
          predicate_column_datatypes_count[predicate_datatype]++;
        }

        if (pqp->lqp_node->type == LQPNodeType::Join) {
          const auto& join_node = static_cast<const JoinNode&>(*pqp->lqp_node);

          const auto primary_operator_join_predicate = OperatorJoinPredicate::from_expression(
              *join_node.join_predicates()[0], *join_node.left_input(), *join_node.right_input());

          if (!primary_operator_join_predicate.has_value()) {
            return PQPVisitation::VisitInputs;
          }
          auto left_columnid = primary_operator_join_predicate->column_ids.first;

          // auto join_datatype = pqp->left_input_table()->table_statistics()->column_data_type(left_columnid);
          auto stats = default_cardinality_estimator->estimate_statistics(join_node.left_input());

          auto join_datatype = stats.table_statistics;
          if (left_columnid > join_datatype->column_statistics.size()) {
            std::cout << "Join ColumnId: " << left_columnid
                      << " is out of bounds for table statistics with size: " << join_datatype->column_statistics.size()
                      << '\n';
            auto right_columnid = primary_operator_join_predicate->column_ids.second;
            auto stats2 = default_cardinality_estimator->estimate_statistics(join_node.right_input());
            auto right_join_datatype = stats2.table_statistics;
            if (right_columnid > right_join_datatype->column_statistics.size()) {
              std::cout << "Right Join ColumnId: " << right_columnid
                        << " is out of bounds for table statistics with size: "
                        << right_join_datatype->column_statistics.size();
            } else {
              auto right_join_column_datatype = right_join_datatype->column_data_type(right_columnid);
              join_column_datatype_count[right_join_column_datatype]++;
            }
            return PQPVisitation::VisitInputs;
          }
          auto datatype = join_datatype->column_data_type(left_columnid);
          join_column_datatype_count[datatype]++;
        }
      }
      return PQPVisitation::VisitInputs;
    });

    _metrics->join_column_datatype = join_column_datatype_count;
    _metrics->predicate_column_datatype = predicate_column_datatypes_count;

    // std::cout << _metrics->operator_cardinality_metrics.size() << " operator cardinality metrics collected."
    //             << std::endl;

    // std::cout << "Cardinality: " << _metrics->true_cardinality << std::endl;
    _root_operator_task->get_operator()->clear_output();
  }

  if (!_result_table) {
    _query_has_output = false;
  }

  return {SQLPipelineStatus::Success, _result_table};
}

const std::shared_ptr<TransactionContext>& SQLPipelineStatement::transaction_context() const {
  return _transaction_context;
}

const std::shared_ptr<SQLPipelineStatementMetrics>& SQLPipelineStatement::metrics() const {
  return _metrics;
}

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
      const auto file = std::ifstream{import->filename};
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

}  // namespace hyrise
