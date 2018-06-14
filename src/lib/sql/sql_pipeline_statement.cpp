#include "sql_pipeline_statement.hpp"

#include <boost/algorithm/string.hpp>

#include <iomanip>
#include <utility>

#include "SQLParser.h"
#include "concurrency/transaction_manager.hpp"
#include "optimizer/optimizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/hsql_expr_translator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_query_plan.hpp"
#include "sql/sql_translator.hpp"
#include "utils/assert.hpp"

namespace opossum {

SQLPipelineStatement::SQLPipelineStatement(const std::string& sql, std::shared_ptr<hsql::SQLParserResult> parsed_sql,
                                           const UseMvcc use_mvcc,
                                           const std::shared_ptr<TransactionContext>& transaction_context,
                                           const std::shared_ptr<LQPTranslator>& lqp_translator,
                                           const std::shared_ptr<Optimizer>& optimizer,
                                           const PreparedStatementCache& prepared_statements)
    : _sql_string(sql),
      _use_mvcc(use_mvcc),
      _auto_commit(_use_mvcc == UseMvcc::Yes && !transaction_context),
      _transaction_context(transaction_context),
      _lqp_translator(lqp_translator),
      _optimizer(optimizer),
      _parsed_sql_statement(std::move(parsed_sql)),
      _metrics(std::make_shared<SQLPipelineStatementMetrics>()),
      _prepared_statements(prepared_statements) {
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
    // Return cached result
    return _parsed_sql_statement;
  }

  DebugAssert(!_sql_string.empty(), "Cannot parse empty SQL string");

  _parsed_sql_statement = std::make_shared<hsql::SQLParserResult>();
  try {
    hsql::SQLParser::parse(_sql_string, _parsed_sql_statement.get());
  } catch (const std::exception& exception) {
    throw std::runtime_error("Error while parsing SQL query:\n  " + std::string(exception.what()));
  }

  if (!_parsed_sql_statement->isValid()) {
    throw std::runtime_error(SQLPipelineStatement::create_parse_error_message(_sql_string, *_parsed_sql_statement));
  }

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

  const auto* statement = parsed_sql->getStatement(0);
  if (const auto prepared_statement = dynamic_cast<const hsql::PrepareStatement*>(statement)) {
    // If this is as PreparedStatement, we want to translate the actual query and not the PREPARE FROM ... part.
    // However, that part is not yet parsed, so we need to parse the raw string from the PreparedStatement.
    Assert(_prepared_statements, "Cannot prepare statement without prepared statement cache.");
    parsed_sql = SQLPipelineBuilder{prepared_statement->query}.create_pipeline_statement().get_parsed_sql_statement();
    _num_parameters = static_cast<uint16_t>(parsed_sql->parameters().size());
  }

  const auto lqp_roots = SQLTranslator{_use_mvcc == UseMvcc::Yes}.translate_parse_result(*parsed_sql);
  DebugAssert(lqp_roots.size() == 1, "LQP translation returned no or more than one LQP root for a single statement.");
  _unoptimized_logical_plan = lqp_roots.front();

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->translate_time_micros = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  return _unoptimized_logical_plan;
}

const std::shared_ptr<AbstractLQPNode>& SQLPipelineStatement::get_optimized_logical_plan() {
  if (_optimized_logical_plan) {
    return _optimized_logical_plan;
  }

  const auto& unoptimized_lqp = get_unoptimized_logical_plan();

  const auto started = std::chrono::high_resolution_clock::now();

  _optimized_logical_plan = _optimizer->optimize(unoptimized_lqp);

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->optimize_time_micros = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  // The optimizer works on the original unoptimized LQP nodes. After optimizing, the unoptimized version is also
  // optimized, which could lead to subtle bugs. optimized_logical_plan holds the original values now.
  // As the unoptimized LQP is only used for visualization, we can afford to recreate it if necessary.
  _unoptimized_logical_plan = nullptr;

  return _optimized_logical_plan;
}

const std::shared_ptr<SQLQueryPlan>& SQLPipelineStatement::get_query_plan() {
  if (_query_plan) {
    return _query_plan;
  }

  // If we need a transaction context but haven't passed one in, this is the latest point where we can create it
  if (!_transaction_context && _use_mvcc == UseMvcc::Yes) {
    _transaction_context = TransactionManager::get().new_transaction_context();
  }

  _query_plan = std::make_shared<SQLQueryPlan>();

  // Stores when the actual compilation started/ended
  auto started = std::chrono::high_resolution_clock::now();
  auto done = started;  // dummy value needed for initialization

  const auto* statement = get_parsed_sql_statement()->getStatement(0);

  auto assert_same_mvcc_mode = [this](const SQLQueryPlan& plan) {
    if (plan.tree_roots().front()->transaction_context_is_set()) {
      Assert(_use_mvcc == UseMvcc::Yes, "Trying to use MVCC cached query without a transaction context.");
    } else {
      Assert(_use_mvcc == UseMvcc::No, "Trying to use non-MVCC cached query with a transaction context.");
    }
  };

  if (const auto cached_plan = SQLQueryCache<SQLQueryPlan>::get().try_get(_sql_string)) {
    // Handle query plan if statement has been cached
    auto& plan = *cached_plan;

    DebugAssert(!plan.tree_roots().empty(), "QueryPlan retrieved from cache is empty.");
    assert_same_mvcc_mode(plan);

    _query_plan->append_plan(plan.recreate());
    _metrics->query_plan_cache_hit = true;
    done = std::chrono::high_resolution_clock::now();
  } else if (const auto* execute_statement = dynamic_cast<const hsql::ExecuteStatement*>(statement)) {
    // Handle query plan if we are executing a prepared statement
    Assert(_prepared_statements, "Cannot execute statement without prepared statement cache.");
    const auto plan = _prepared_statements->try_get(execute_statement->name);

    Assert(plan, "Requested prepared statement does not exist!");
    assert_same_mvcc_mode(*plan);

    // Get list of arguments from EXECUTE statement.
    std::vector<AllParameterVariant> arguments;
    if (execute_statement->parameters != nullptr) {
      for (const auto* expr : *execute_statement->parameters) {
        arguments.push_back(HSQLExprTranslator::to_all_parameter_variant(*expr));
      }
    }

    Assert(arguments.size() == plan->num_parameters(),
           "Number of arguments provided does not match expected number of arguments.");

    _query_plan->append_plan(plan->recreate(arguments));
    done = std::chrono::high_resolution_clock::now();
  } else {
    // "Normal" mode in which the query plan is created
    const auto& lqp = get_optimized_logical_plan();

    // Reset time to exclude previous pipeline steps
    started = std::chrono::high_resolution_clock::now();
    _query_plan->add_tree_by_root(_lqp_translator->translate_node(lqp));

    // Set number of parameters to match later in case of prepared statement
    _query_plan->set_num_parameters(_num_parameters);
    done = std::chrono::high_resolution_clock::now();
  }

  if (_use_mvcc == UseMvcc::Yes) _query_plan->set_transaction_context(_transaction_context);

  if (const auto* prepared_statement = dynamic_cast<const hsql::PrepareStatement*>(statement)) {
    Assert(_prepared_statements, "Cannot prepare statement without prepared statement cache.");
    _prepared_statements->set(prepared_statement->name, *_query_plan);
  }

  // Cache newly created plan for the according sql statement (only if not already cached)
  if (!_metrics->query_plan_cache_hit) {
    SQLQueryCache<SQLQueryPlan>::get().set(_sql_string, *_query_plan);
  }

  _metrics->compile_time_micros = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  return _query_plan;
}

const std::vector<std::shared_ptr<OperatorTask>>& SQLPipelineStatement::get_tasks() {
  if (!_tasks.empty()) {
    return _tasks;
  }

  const auto& query_plan = get_query_plan();
  DebugAssert(query_plan->tree_roots().size() == 1,
              "Physical query plan creation returned no or more than one plan for a single statement.");

  const auto& root = query_plan->tree_roots().front();
  _tasks = OperatorTask::make_tasks_from_operator(root);
  return _tasks;
}

const std::shared_ptr<const Table>& SQLPipelineStatement::get_result_table() {
  if (_result_table || !_query_has_output) {
    return _result_table;
  }

  const auto& tasks = get_tasks();

  const auto started = std::chrono::high_resolution_clock::now();

  const auto* statement = get_parsed_sql_statement()->getStatement(0);
  // If this is a PREPARE x FROM ... command, calling get_result_table should not fail but just return
  if (statement->isType(hsql::kStmtPrepare)) {
    _query_has_output = false;
    const auto done = std::chrono::high_resolution_clock::now();
    _metrics->execution_time_micros = std::chrono::duration_cast<std::chrono::microseconds>(done - started);
    return _result_table;
  }

  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  if (_auto_commit) {
    _transaction_context->commit();
  }

  const auto done = std::chrono::high_resolution_clock::now();
  _metrics->execution_time_micros = std::chrono::duration_cast<std::chrono::microseconds>(done - started);

  // Get output from the last task
  _result_table = tasks.back()->get_operator()->get_output();
  if (_result_table == nullptr) _query_has_output = false;

  return _result_table;
}

const std::shared_ptr<TransactionContext>& SQLPipelineStatement::transaction_context() const {
  return _transaction_context;
}

std::string SQLPipelineStatement::create_parse_error_message(const std::string& sql,
                                                             const hsql::SQLParserResult& result) {
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

const std::shared_ptr<SQLPipelineStatementMetrics>& SQLPipelineStatement::metrics() const { return _metrics; }
}  // namespace opossum
