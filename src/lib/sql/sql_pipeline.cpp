#include <boost/algorithm/string.hpp>

#include "SQLParser.h"
#include "sql_pipeline.hpp"

namespace opossum {

SQLPipeline::SQLPipeline(const std::string& sql, const UseMvcc use_mvcc, const std::shared_ptr<Optimizer>& optimizer)
    : SQLPipeline(sql, nullptr, use_mvcc, optimizer) {}

SQLPipeline::SQLPipeline(const std::string& sql, std::shared_ptr<opossum::TransactionContext> transaction_context,
                         const std::shared_ptr<Optimizer>& optimizer)
    : SQLPipeline(sql, transaction_context, UseMvcc::Yes, optimizer) {
  DebugAssert(transaction_context != nullptr, "Cannot pass nullptr as explicit transaction context.");
  DebugAssert(transaction_context->phase() == TransactionPhase::Active,
              "The transaction context cannot have been committed already.");
}

// Private constructor
SQLPipeline::SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context,
                         const UseMvcc use_mvcc, const std::shared_ptr<Optimizer>& optimizer)
    : _transaction_context(transaction_context), _optimizer(optimizer) {
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

  std::vector<std::shared_ptr<hsql::SQLParserResult>> parsed_statements;
  for (auto* statement : parse_result.releaseStatements()) {
    parsed_statements.emplace_back(std::make_shared<hsql::SQLParserResult>(statement));
  }

  auto seen_altering_statement = false;

  // We want to split the (multi-) statement SQL string into the strings for each statement. We can then use those
  // statement strings to cache query plans.
  // The sql parser only offers us the length of the string, so we need to split it manually.
  auto sql_string_offset = 0u;

  for (auto& parsed_statement : parsed_statements) {
    parsed_statement->setIsValid(true);

    // We will always have one at 0 because we set it ourselves
    const auto* statement = parsed_statement->getStatement(0);

    switch (statement->type()) {
      // Check if statement alters the structure of the database in a way that following statements might depend upon.
      case hsql::StatementType::kStmtImport:
      case hsql::StatementType::kStmtCreate:
      case hsql::StatementType::kStmtDrop:
      case hsql::StatementType::kStmtAlter:
      case hsql::StatementType::kStmtRename: {
        seen_altering_statement = true;
        break;
      }
      default: { /* do nothing */
      }
    }

    // Get the statement string from the original query string, so we can pass it to the SQLPipelineStatement
    const auto statement_string_length = statement->stringLength;
    const auto statement_string = boost::trim_copy(sql.substr(sql_string_offset, statement_string_length));
    sql_string_offset += statement_string_length;

    auto pipeline_statement = std::make_shared<SQLPipelineStatement>(statement_string, std::move(parsed_statement),
                                                                     use_mvcc, transaction_context, optimizer);
    _sql_pipeline_statements.push_back(std::move(pipeline_statement));
  }

  // If we see at least one structure altering statement and we have more than one statement, we require execution of a
  // statement before the next one can be translated (so the next statement sees the previous structural changes).
  _requires_execution = seen_altering_statement && statement_count() > 1;
}

const std::vector<std::string>& SQLPipeline::get_sql_strings() {
  if (!_sql_strings.empty()) {
    return _sql_strings;
  }

  _sql_strings.reserve(statement_count());
  for (auto& pipeline_statement : _sql_pipeline_statements) {
    _sql_strings.push_back(pipeline_statement->get_sql_string());
  }

  return _sql_strings;
}

const std::vector<std::shared_ptr<hsql::SQLParserResult>>& SQLPipeline::get_parsed_sql_statements() {
  if (!_parsed_sql_statements.empty()) {
    return _parsed_sql_statements;
  }

  _parsed_sql_statements.reserve(statement_count());
  for (auto& pipeline_statement : _sql_pipeline_statements) {
    try {
      _parsed_sql_statements.push_back(pipeline_statement->get_parsed_sql_statement());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline_statement;

      // Don't keep bad values
      _parsed_sql_statements.clear();
      throw;
    }
  }

  return _parsed_sql_statements;
}
const std::vector<std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_unoptimized_logical_plans() {
  if (!_unoptimized_logical_plans.empty()) {
    return _unoptimized_logical_plans;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "One or more SQL statement is dependent on the execution of a previous one. "
         "Cannot translate all statements without executing, i.e. calling get_result_table()");

  _unoptimized_logical_plans.reserve(statement_count());
  for (auto& pipeline_statement : _sql_pipeline_statements) {
    try {
      _unoptimized_logical_plans.push_back(pipeline_statement->get_unoptimized_logical_plan());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline_statement;

      // Don't keep bad values
      _unoptimized_logical_plans.clear();
      throw;
    }
  }

  return _unoptimized_logical_plans;
}

const std::vector<std::shared_ptr<AbstractLQPNode>>& SQLPipeline::get_optimized_logical_plans() {
  if (!_optimized_logical_plans.empty()) {
    return _optimized_logical_plans;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "One or more SQL statement is dependent on the execution of a previous one. "
         "Cannot translate all statements without executing, i.e. calling get_result_table()");

  _optimized_logical_plans.reserve(statement_count());
  for (auto& pipeline_statement : _sql_pipeline_statements) {
    try {
      _optimized_logical_plans.push_back(pipeline_statement->get_optimized_logical_plan());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline_statement;

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

const std::vector<std::shared_ptr<SQLQueryPlan>>& SQLPipeline::get_query_plans() {
  if (!_query_plans.empty()) {
    return _query_plans;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "One or more SQL statement is dependent on the execution of a previous one. "
         "Cannot compile all statements without executing, i.e. calling get_result_table()");

  _query_plans.reserve(statement_count());
  for (auto& pipeline_statement : _sql_pipeline_statements) {
    try {
      _query_plans.push_back(pipeline_statement->get_query_plan());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline_statement;

      // Don't keep bad values
      _query_plans.clear();
      throw;
    }
  }

  return _query_plans;
}

const std::vector<std::vector<std::shared_ptr<OperatorTask>>>& SQLPipeline::get_tasks() {
  if (!_tasks.empty()) {
    return _tasks;
  }

  Assert(!_requires_execution || _pipeline_was_executed,
         "One or more SQL statement is dependent on the execution of a previous one. "
         "Cannot generate tasks for all statements without executing, i.e. calling get_result_table()");

  _tasks.reserve(statement_count());
  for (auto& pipeline_statement : _sql_pipeline_statements) {
    try {
      _tasks.push_back(pipeline_statement->get_tasks());
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline_statement;

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

  for (auto& pipeline_statement : _sql_pipeline_statements) {
    try {
      pipeline_statement->get_result_table();
    } catch (const std::exception& exception) {
      _failed_pipeline_statement = pipeline_statement;
      throw;
    }
  }

  _result_table = _sql_pipeline_statements.back()->get_result_table();
  _pipeline_was_executed = true;

  return _result_table;
}

const std::shared_ptr<TransactionContext>& SQLPipeline::transaction_context() const { return _transaction_context; }

const std::shared_ptr<SQLPipelineStatement>& SQLPipeline::failed_pipeline_statement() const {
  return _failed_pipeline_statement;
}

size_t SQLPipeline::statement_count() const { return _sql_pipeline_statements.size(); }

bool SQLPipeline::requires_execution() const { return _requires_execution; }

std::chrono::microseconds SQLPipeline::compile_time_microseconds() {
  if (_compile_time_microseconds.count() > 0) {
    return _compile_time_microseconds;
  }

  if (_requires_execution || _query_plans.empty()) {
    Assert(_pipeline_was_executed,
           "Cannot get compile time without having compiled or having executed a multi-statement query");
  }

  for (const auto& pipeline_statement : _sql_pipeline_statements) {
    _compile_time_microseconds += pipeline_statement->compile_time_microseconds();
  }

  return _compile_time_microseconds;
}
std::chrono::microseconds SQLPipeline::execution_time_microseconds() {
  Assert(_pipeline_was_executed, "Cannot return execution duration without having executed.");

  if (_execution_time_microseconds.count() == 0) {
    for (const auto& pipeline_statement : _sql_pipeline_statements) {
      _execution_time_microseconds += pipeline_statement->execution_time_microseconds();
    }
  }

  return _execution_time_microseconds;
}

}  // namespace opossum
