#include <utility>

#include "optimizer/optimizer.hpp"
#include "SQLParser.h"
#include "sql_pipeline.hpp"
#include "utils/assert.hpp"
#include "sql_translator.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "concurrency/transaction_manager.hpp"

namespace opossum {

SQLPipeline::SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context) : _sql(sql) {
  if (transaction_context != nullptr) {
    _transaction_context = transaction_context;
  } else {
    _transaction_context = TransactionManager::get().new_transaction_context();
  }
}

const hsql::SQLParserResult& SQLPipeline::get_parsed_sql() {
  if (_parsed_sql) {
    // Returned cached result
    return *(_parsed_sql.get());
  }

  auto parse_result = std::make_unique<hsql::SQLParserResult>();
  try {
    hsql::SQLParser::parse(_sql, parse_result.get());
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while parsing SQL query:\n  " + std::string(exception.what()));
  }

  if (!parse_result->isValid()) {
    throw std::logic_error("SQL query not valid.");
  }

  _parsed_sql = std::move(parse_result);
  return *(_parsed_sql.get());
}

const std::vector<std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_unoptimized_logical_plan(const bool validate) {
  if (!_unopt_logical_plan.empty()) {
    return _unopt_logical_plan;
  }

  const auto& parsed_sql = get_parsed_sql();
  try {
    _unopt_logical_plan = SQLTranslator{validate}.translate_parse_result(parsed_sql);
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while compiling query plan:\n  " + std::string(exception.what()));
  }

  return _unopt_logical_plan;
}

const std::vector<std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_logical_plan(const bool validate) {
  if (!_logical_plan.empty()) {
    return _logical_plan;
  }

  auto unopt_lqp = get_unoptimized_logical_plan(validate);
  _logical_plan.reserve(unopt_lqp.size());

  try {
    for (const auto& node : unopt_lqp) {
      _logical_plan.push_back(Optimizer::get().optimize(node));
    }
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while optimizing query plan:\n  " + std::string(exception.what()));
  }

  return _logical_plan;
}

const SQLQueryPlan& SQLPipeline::get_query_plan() {
  if (_query_plan) {
    return *(_query_plan.get());
  }

  const auto& lqp_roots = get_logical_plan();
  auto plan = std::make_unique<SQLQueryPlan>();\

  try {
    for (const auto& node : lqp_roots) {
      auto optimized = Optimizer::get().optimize(node);
      auto op = LQPTranslator{}.translate_node(optimized);
      plan->add_tree_by_root(op);
    }
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while translating query plan:\n  " + std::string(exception.what()));
  }

  plan->set_transaction_context(_transaction_context);

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
  if (_result_table) {
    return _result_table;
  }

  const auto& op_tasks = get_tasks();
  try {
    for (const auto& task : op_tasks) {
      task->schedule();
    }
  } catch (const std::exception& exception) {
    _transaction_context->rollback();
    throw std::runtime_error("Error while executing tasks:\n  " + std::string(exception.what()));
  }

  _result_table = op_tasks.back()->get_operator()->get_output();
  return _result_table;
}

const std::shared_ptr<TransactionContext>& SQLPipeline::transaction_context() {
  return _transaction_context;
}

} // namespace opossum
