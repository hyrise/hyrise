#pragma once

#include <memory>
#include <string>

#include "types.hpp"

#include "sql_pipeline.hpp"
#include "sql_pipeline_statement.hpp"

namespace opossum {

class Optimizer;

/**
 * Interface for the configured execution of SQL.
 *
 * Minimal usage:
 *      SQL{"SELECT * FROM t;"}.pipeline().get_result_table()
 *
 * With custom Optimizer and TransactionContext:
 *      SQL{query}.
 *          set_optimizer(optimizer).
 *          set_transaction_context(tc).
 *          pipeline();
 *
 * Defaults:
 *  - MVCC is enabled
 *  - The default Optimizer (Optimizer::create_default_optimizer() is used.
 *
 * Favour this interface over calling the SQLPipeline[Statement] constructors with their long parameter list.
 * See SQLPipeline[Statement] doc for these classes, in short SQLPipeline ist for queries with multiple statement,
 * SQLPipelineStatement for single statement queries.
 */
class SQL final {
 public:
  explicit SQL(const std::string& sql);

  SQL& set_use_mvcc(const UseMvcc use_mvcc);
  SQL& set_optimizer(const std::shared_ptr<Optimizer>& optimizer);
  SQL& set_prepared_statement_cache(const PreparedStatementCache& prepared_statements);
  SQL& set_transaction_context(const std::shared_ptr<TransactionContext>& transaction_context);

  /**
   * Short for set_use_mvcc(UseMvcc::No)
   */
  SQL& disable_mvcc();

  SQLPipeline pipeline() const;
  SQLPipelineStatement pipeline_statement() const;

 private:
  const std::string _sql;

  UseMvcc _use_mvcc{UseMvcc::Yes};
  std::shared_ptr<TransactionContext> _transaction_context;
  std::shared_ptr<Optimizer> _optimizer;
  PreparedStatementCache _prepared_statements;
};

}  // namespace opossum
