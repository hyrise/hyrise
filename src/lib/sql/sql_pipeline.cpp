#include <boost/algorithm/string.hpp>

#include "SQLParser.h"
#include "sql_pipeline.hpp"

namespace opossum {

SQLPipeline::SQLPipeline(const std::string& sql, bool use_mvcc) : SQLPipeline(sql, nullptr, use_mvcc) {}

SQLPipeline::SQLPipeline(const std::string& sql, std::shared_ptr<opossum::TransactionContext> transaction_context)
    : SQLPipeline(sql, std::move(transaction_context), true) {
  DebugAssert(_sql_pipeline_statements.front()->transaction_context() != nullptr,
              "Cannot pass nullptr as explicit transaction context.");
}

// Private constructor
SQLPipeline::SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context, bool use_mvcc)
    : _transaction_context(std::move(transaction_context)) {
  hsql::SQLParserResult parse_result;
  try {
    hsql::SQLParser::parse(sql, &parse_result);
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while parsing SQL query:\n  " + std::string(exception.what()));
  }

  if (!parse_result.isValid()) {
    throw std::runtime_error(SQLPipelineStatement::create_parse_error_message(sql, parse_result));
  }

  DebugAssert(parse_result.size() > 0, "Cannot create empty SQLPipeline.");
  _sql_pipeline_statements.reserve(parse_result.size());

  bool seen_altering_statement = false;

  for (auto* statement : parse_result.releaseStatements()) {
    switch (statement->type()) {
      // Check if statement alters table in any way that would possibly not be seen in a later statement.
      case hsql::StatementType::kStmtImport:
      case hsql::StatementType::kStmtCreate:
      case hsql::StatementType::kStmtDrop:
      case hsql::StatementType::kStmtAlter:
      case hsql::StatementType::kStmtRename:
        seen_altering_statement = true;
      default: { /* do nothing */
      }
    }

    auto parsed_statement = std::make_shared<hsql::SQLParserResult>(statement);
    // We know this is always true because the check above would fail otherwise
    parsed_statement->setIsValid(true);

    auto pipeline_statement =
        std::make_shared<SQLPipelineStatement>(std::move(parsed_statement), _transaction_context, use_mvcc);
    _sql_pipeline_statements.push_back(std::move(pipeline_statement));
  }

  _num_statements = _sql_pipeline_statements.size();

  // If we see at least one altering statement and we have more than one statement, we require execution of a statement
  // before the next one can be translated.
  _requires_execution = seen_altering_statement && _num_statements > 1;
}

const std::vector<const std::shared_ptr<hsql::SQLParserResult>>& SQLPipeline::get_parsed_sql_statements() {
  if (!_parsed_sql_statements.empty()) {
    return _parsed_sql_statements;
  }

  _parsed_sql_statements.reserve(_num_statements);
  for (auto& pipeline : _sql_pipeline_statements) {
    try {
      _parsed_sql_statements.push_back(pipeline->get_parsed_sql_statement());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline;

      // Don't keep bad values
      _parsed_sql_statements.clear();
      throw;
    }
  }

  return _parsed_sql_statements;
}
const std::vector<const std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_unoptimized_logical_plans() {
  if (!_unoptimized_logical_plans.empty()) {
    return _unoptimized_logical_plans;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "Cannot translate all statements without executing the previous ones.");

  _unoptimized_logical_plans.reserve(_num_statements);
  for (auto& pipeline : _sql_pipeline_statements) {
    try {
      _unoptimized_logical_plans.push_back(pipeline->get_unoptimized_logical_plan());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline;

      // Don't keep bad values
      _unoptimized_logical_plans.clear();
      throw;
    }
  }

  return _unoptimized_logical_plans;
}

const std::vector<const std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_optimized_logical_plans() {
  if (!_optimized_logical_plans.empty()) {
    return _optimized_logical_plans;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "Cannot translate all statements without executing the previous ones.");

  _optimized_logical_plans.reserve(_num_statements);
  for (auto& pipeline : _sql_pipeline_statements) {
    try {
      _optimized_logical_plans.push_back(pipeline->get_unoptimized_logical_plan());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline;

      // Don't keep bad values
      _optimized_logical_plans.clear();
      throw;
    }
  }

  // The optimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. optimized_logical_plan holds the original values now.
  // As the unoptimized LQP is only used for visualization, we can afford to recreate it if necessary.
  _unoptimized_logical_plans.clear();

  return _optimized_logical_plans;
}

const std::vector<const std::shared_ptr<SQLQueryPlan>>& SQLPipeline::get_query_plans() {
  if (!_query_plans.empty()) {
    return _query_plans;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "Cannot compile all statements without executing the previous ones.");

  _query_plans.reserve(_num_statements);
  for (auto& pipeline : _sql_pipeline_statements) {
    try {
      _query_plans.push_back(pipeline->get_query_plan());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline;

      // Don't keep bad values
      _query_plans.clear();
      throw;
    }
  }

  return _query_plans;
}

const std::vector<const std::vector<std::shared_ptr<OperatorTask>>>& SQLPipeline::get_tasks() {
  if (!_tasks.empty()) {
    return _tasks;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "Cannot generate tasks for all statements without executing the previous ones.");

  _tasks.reserve(_num_statements);
  for (auto& pipeline : _sql_pipeline_statements) {
    try {
      _tasks.push_back(pipeline->get_tasks());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline;

      // Don't keep bad values
      _tasks.clear();
      throw;
    }
  }

  return _tasks;
}

const std::shared_ptr<const Table>& SQLPipeline::get_result_table() {
  if (_pipeline_was_executed) {
    return _result_table;
  }

  for (auto& pipeline : _sql_pipeline_statements) {
    try {
      pipeline->get_result_table();
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline;
      throw;
    }
  }

  _result_table = _sql_pipeline_statements.back()->get_result_table();
  _pipeline_was_executed = true;

  return _result_table;
}

const std::shared_ptr<TransactionContext>& SQLPipeline::transaction_context() const { return _transaction_context; }

const std::shared_ptr<SQLPipelineStatement>& SQLPipeline::failed_pipeline_statement() {
  return _failed_pipeline_statement;
}

size_t SQLPipeline::num_statements() { return _num_statements; }

bool SQLPipeline::requires_execution() { return _requires_execution; }

std::chrono::microseconds SQLPipeline::compile_time_microseconds() {
  if (_compile_time_microseconds.count() > 0) {
    return _compile_time_microseconds;
  }

  if (_requires_execution || _query_plans.empty()) {
    Assert(_pipeline_was_executed,
           "Cannot get compile time without having compiled or having executed a multi-statement query");
  }

  for (const auto& pipeline : _sql_pipeline_statements) {
    _compile_time_microseconds += pipeline->compile_time_microseconds();
  }

  return _compile_time_microseconds;
}
std::chrono::microseconds SQLPipeline::execution_time_microseconds() {
  Assert(_pipeline_was_executed, "Cannot return execution duration without having executed.");

  if (_execution_time_microseconds.count() == 0) {
    for (const auto& pipeline : _sql_pipeline_statements) {
      _execution_time_microseconds += pipeline->execution_time_microseconds();
    }
  }

  return _execution_time_microseconds;
}

}  // namespace opossum
