#include "sql.hpp"

namespace opossum {

SQL::SQL(const std::string& sql): _sql(sql) {}

SQL& SQL::set_use_mvcc(const UseMvcc use_mvcc) {
  _use_mvcc = use_mvcc;
  return *this;
}

SQL& SQL::set_optimizer(const std::shared_ptr<Optimizer>& optimizer) {
  _optimizer = optimizer;
  return *this;
}

SQL& SQL::set_prepared_statement_cache(const PreparedStatementCache& prepared_statements) {
  _prepared_statements = prepared_statements;
  return *this;
}

SQL& SQL::set_transaction_context(const std::shared_ptr<TransactionContext>& transaction_context) {
  _transaction_context = transaction_context;
  _use_mvcc = UseMvcc::Yes;

  return *this;
}

SQLPipeline SQL::pipeline() const {
  return {_sql, _transaction_context, _use_mvcc, _optimizer, _prepared_statements};
}

SQLPipelineStatement SQL::pipeline_statement() const {
  return {_sql, nullptr, _use_mvcc, _transaction_context, _optimizer, _prepared_statements};
}

}  // namespace opossum
