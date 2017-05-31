#include "sql_query_operator.hpp"

#include <memory>
#include <string>

#include "sql/sql_query_translator.hpp"

#include "SQLParser.h"

namespace opossum {

SQLParseTreeCache SQLQueryOperator::_parse_tree_cache = SQLParseTreeCache(0);

SQLParseTreeCache SQLQueryOperator::_prepared_stmts = SQLParseTreeCache(1024);

SQLQueryOperator::SQLQueryOperator(const std::string& query) : _query(query) {
  _result_op = std::make_shared<SQLResultOperator>();
  _result_task = std::make_shared<OperatorTask>(_result_op);
}

const std::string SQLQueryOperator::name() const { return "SQLQueryOperator"; }

uint8_t SQLQueryOperator::num_in_tables() const { return 0; }

uint8_t SQLQueryOperator::num_out_tables() const { return 0; }

const std::shared_ptr<OperatorTask>& SQLQueryOperator::get_result_task() const { return _result_task; }

std::shared_ptr<const Table> SQLQueryOperator::on_execute(std::shared_ptr<TransactionContext> context) {
  // TODO(torpedro): Check query cache for execution plan.

  SQLQueryTranslator translator;

  std::shared_ptr<hsql::SQLParserResult> result;

  // Check query cache for parse tree.
  if (_parse_tree_cache.has(_query)) {
    result = _parse_tree_cache.get(_query);

  } else {
    // Parse the query.
    result = std::make_shared<hsql::SQLParserResult>();
    if (!translator.parse_query(_query, result.get())) {
      throw translator.get_error_msg();
    }
  }

  translate_stmts(result);

  return nullptr;
}

void SQLQueryOperator::translate_stmts(std::shared_ptr<hsql::SQLParserResult> result) {
  SQLQueryTranslator translator;
  const std::vector<hsql::SQLStatement*>& statements = result->getStatements();

  for (const hsql::SQLStatement* stmt : statements) {
    switch (stmt->type()) {
      case hsql::kStmtPrepare: {
        const hsql::PrepareStatement* prepare = (const hsql::PrepareStatement*)stmt;

        SQLQueryTranslator translator;
        std::shared_ptr<hsql::SQLParserResult> result;
        result = std::make_shared<hsql::SQLParserResult>();
        if (!translator.parse_query(prepare->query, result.get())) {
          throw translator.get_error_msg();
        }

        // Cache the result.
        _prepared_stmts.set(prepare->name, result);
        break;
      }
      case hsql::kStmtExecute: {
        const hsql::ExecuteStatement* execute = (const hsql::ExecuteStatement*)stmt;

        if (_prepared_stmts.has(execute->name)) {
          std::shared_ptr<hsql::SQLParserResult> result = _prepared_stmts.get(execute->name);
          translator.translate_parse_result(*result);
        } else {
          throw "Requested prepared statement does not exist!";
        }
        break;
      }
      default: {
        if (!translator.translate_statement(*stmt)) {
          throw translator.get_error_msg();
        }
        break;
      }
    }
  }

  // Schedule all tasks.
  auto tasks = translator.get_tasks();

  if (tasks.size() > 0) {
    tasks.back()->set_as_predecessor_of(_result_task);
    _result_op->set_input_operator(tasks.back()->get_operator());

    for (const auto& task : tasks) {
      task->schedule();
    }
    _result_task->schedule();
  }
}

}  // namespace opossum
