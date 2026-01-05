#include "sql_pipeline_builder.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "hyrise.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_plan_cache.hpp"
#include "types.hpp"

namespace hyrise {

SQLPipelineBuilder::SQLPipelineBuilder(const std::string& sql)
    : _sql(sql), _pqp_cache(Hyrise::get().default_pqp_cache), _lqp_cache(Hyrise::get().default_lqp_cache) {}

SQLPipelineBuilder& SQLPipelineBuilder::with_mvcc(const UseMvcc use_mvcc) {
  _use_mvcc = use_mvcc;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_optimizer(const std::shared_ptr<Optimizer>& optimizer) {
  _optimizer = optimizer;
  return *this;
}

SQLPipelineBuilder& SQLPipelineBuilder::with_data_dependency_optimizer(
    const std::shared_ptr<Optimizer>& data_dependency_optimizer) {
  _data_dependency_optimizer = data_dependency_optimizer;
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

SQLPipelineBuilder& SQLPipelineBuilder::disable_mvcc() {
  return with_mvcc(UseMvcc::No);
}

SQLPipeline SQLPipelineBuilder::create_pipeline() const {
  std::string optimizer_optimization = std::getenv("DD_OPTIMIZER");

  bool use_data_dependency_optimizer = optimizer_optimization == "ON";

  auto optimizer = _optimizer ? _optimizer : Optimizer::create_default_optimizer(use_data_dependency_optimizer);

  auto data_dependency_optimizer =
      _data_dependency_optimizer
          ? _data_dependency_optimizer
          : Optimizer::create_default_optimizer_with_cardinality_estimator();
  std::cout << "initialized sql pipeline with two optimizers" << std::endl;
  auto pipeline = SQLPipeline(_sql, _transaction_context, _use_mvcc, optimizer, _pqp_cache, _lqp_cache, data_dependency_optimizer);
  return pipeline;
}

}  // namespace hyrise
