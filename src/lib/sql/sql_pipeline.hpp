#pragma once

#include <string>

#include "storage/table.hpp"
#include "SQLParserResult.h"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "sql/sql_query_plan.hpp"
#include "concurrency/transaction_context.hpp"

namespace opossum {


class SQLPipeline : public Noncopyable {
 public:
  explicit SQLPipeline(const std::string& sql, std::shared_ptr<TransactionContext> transaction_context = nullptr);

  const std::string get_sql_string() { return _sql; }

  const hsql::SQLParserResult& get_parsed_sql();

  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_unoptimized_logical_plan(bool validate = true);

  const std::vector<std::shared_ptr<AbstractLQPNode>>& get_logical_plan(bool validate = true);

  const SQLQueryPlan& get_query_plan();

  const std::vector<std::shared_ptr<OperatorTask>>& get_tasks();

  const std::shared_ptr<const Table>& get_result_table();

  const std::shared_ptr<TransactionContext>& transaction_context();

 private:
  const std::string _sql;

  // Execution results
  std::unique_ptr<hsql::SQLParserResult> _parsed_sql;
  std::vector<std::shared_ptr<AbstractLQPNode>> _unopt_logical_plan;
  std::vector<std::shared_ptr<AbstractLQPNode>> _logical_plan;
  std::unique_ptr<SQLQueryPlan> _query_plan;
  std::vector<std::shared_ptr<OperatorTask>> _op_tasks;
  std::shared_ptr<const Table> _result_table;

  std::shared_ptr<TransactionContext> _transaction_context;

};

}  // namespace opossum