#include "sql_pipeline_builder.hpp"
#include "utils/tracing/probes.hpp"

namespace opossum {

std::shared_ptr<SQLPhysicalPlanCache> SQLPipelineBuilder::default_pqp_cache{};
std::shared_ptr<SQLLogicalPlanCache> SQLPipelineBuilder::default_lqp_cache{};

SQLPipelineBuilder::SQLPipelineBuilder(const std::string& sql)
    : _sql(sql), _pqp_cache(default_pqp_cache), _lqp_cache(default_lqp_cache) {}

SQLPipelineBuilder& SQLPipelineBuilder::with_mvcc(const UseMvcc use_mvcc) {
  _use_mvcc = use_mvcc;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_optimizer(const std::shared_ptr<Optimizer>& optimizer) {
  _optimizer = optimizer;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_transaction_context(
    const std::shared_ptr<TransactionContext>& transaction_context) {
  _transaction_context = transaction_context;
  _use_mvcc = UseMvcc::Yes;

  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_pqp_cache(const std::shared_ptr<SQLPhysicalPlanCache>& pqp_cache) {
  _pqp_cache = pqp_cache;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_lqp_cache(const std::shared_ptr<SQLLogicalPlanCache>& lqp_cache) {
  _lqp_cache = lqp_cache;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::disable_mvcc() { return with_mvcc(UseMvcc::No); }

SQLPipelineBuilder& SQLPipelineBuilder::dont_cleanup_temporaries() {
  _cleanup_temporaries = CleanupTemporaries::No;
  return *this;
}

SQLPipeline SQLPipelineBuilder::create_pipeline() const {
  DTRACE_PROBE1(HYRISE, CREATE_PIPELINE, reinterpret_cast<uintptr_t>(this));
  auto optimizer = _optimizer ? _optimizer : Optimizer::create_default_optimizer();
  auto pipeline =
      SQLPipeline(_sql, _transaction_context, _use_mvcc, optimizer, _pqp_cache, _lqp_cache, _cleanup_temporaries);
  DTRACE_PROBE3(HYRISE, PIPELINE_CREATION_DONE, pipeline.get_sql_per_statement().size(), _sql.c_str(),
                reinterpret_cast<uintptr_t>(this));
  return pipeline;
}

SQLPipelineStatement SQLPipelineBuilder::create_pipeline_statement(
    std::shared_ptr<hsql::SQLParserResult> parsed_sql) const {
  auto optimizer = _optimizer ? _optimizer : Optimizer::create_default_optimizer();

  return {_sql,       std::move(parsed_sql), _use_mvcc, _transaction_context, optimizer, _pqp_cache,
          _lqp_cache, _cleanup_temporaries};
}

}  // namespace opossum
