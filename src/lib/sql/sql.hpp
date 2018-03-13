#pragma once

#include <memory>
#include <string>

#include "types.hpp"

#include "sql_pipeline.hpp"
#include "sql_pipeline_statement.hpp"

namespace opossum {

class Optimizer;

/**
 * Builder for SQLPipeline[Statement]s with configuration options.
 *
 *
 */
class SQL final {
 public:
  explicit SQL(const std::string& sql);

  SQL& set_use_mvcc(const UseMvcc use_mvcc);
  SQL& set_optimizer(const std::shared_ptr<Optimizer>& optimizer);
  SQL& set_prepared_statement_cache(const PreparedStatementCache& prepared_statements);
  SQL& set_transaction_context(const std::shared_ptr<TransactionContext>& transaction_context);

  SQLPipeline pipeline() const;
  SQLPipelineStatement pipeline_statement() const;

 private:
  const std::string _sql;

  UseMvcc _use_mvcc{UseMvcc::No};
  std::shared_ptr<TransactionContext> _transaction_context;
  std::shared_ptr<Optimizer> _optimizer;
  PreparedStatementCache _prepared_statements;
};

}  // namespace opossum
