#include "sql_pipeline_builder.hpp"

namespace opossum {

SQLPipelineBuilder::SQLPipelineBuilder(const std::string& sql) : _sql(sql) {}

SQLPipelineBuilder& SQLPipelineBuilder::with_mvcc(const UseMvcc use_mvcc) {
  _use_mvcc = use_mvcc;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_optimizer(const std::shared_ptr<Optimizer>& optimizer) {
  _optimizer = optimizer;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_prepared_statement_cache(
    const PreparedStatementCache& prepared_statements) {
  _prepared_statements = prepared_statements;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_transaction_context(
    const std::shared_ptr<TransactionContext>& transaction_context) {
  _transaction_context = transaction_context;
  _use_mvcc = UseMvcc::Yes;

  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::disable_mvcc() { return with_mvcc(UseMvcc::No); }

SQLPipeline SQLPipelineBuilder::create_pipeline() const {
  auto optimizer = _optimizer ? _optimizer : Optimizer::create_default_optimizer();

  return {_sql, _transaction_context, _use_mvcc, optimizer, _prepared_statements};
}

SQLPipelineStatement SQLPipelineBuilder::create_pipeline_statement(
    std::shared_ptr<hsql::SQLParserResult> parsed_sql) const {
  auto optimizer = _optimizer ? _optimizer : Optimizer::create_default_optimizer();

  return {_sql, parsed_sql, _use_mvcc, _transaction_context, optimizer, _prepared_statements};
}

}  // namespace opossum
