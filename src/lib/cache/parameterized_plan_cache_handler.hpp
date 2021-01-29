#pragma once

#include "sql/sql_plan_cache.hpp"

namespace opossum {

class ParameterizedPlanCacheHandler {
 public:
  ParameterizedPlanCacheHandler(const std::shared_ptr<SQLLogicalPlanCache>& lqp_cache,
                                const std::shared_ptr<AbstractLQPNode>& unoptimized_lqp,
                                std::chrono::nanoseconds& cache_duration, const UseMvcc use_mvcc);

  std::optional<std::shared_ptr<AbstractLQPNode>> try_get();

  void set(std::shared_ptr<AbstractLQPNode>& optimized_lqp);

 private:
  friend class ParameterizedPlanCacheHandlerTest;
  friend class SQLPipelineStatementTest;
  static const std::tuple<std::shared_ptr<AbstractLQPNode>, std::vector<std::shared_ptr<AbstractExpression>>>
  _split_lqp_values(const std::shared_ptr<AbstractLQPNode>& lqp);

  const std::shared_ptr<SQLLogicalPlanCache>& _lqp_cache;
  std::shared_ptr<AbstractLQPNode> _cache_key;
  std::vector<std::shared_ptr<AbstractExpression>> _extracted_values;
  std::chrono::nanoseconds& _cache_duration;
  const UseMvcc _use_mvcc;
};

}  // namespace opossum
