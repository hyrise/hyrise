#pragma once

#include "sql/sql_plan_cache.hpp"

namespace opossum {

/* The ParameterizedPlanCacheHandler is a facade of the actual lqp cache that makes it possible to cache
 * parameterized plans without much additional cache logic in the sql_pipeline_statement as well as the
 * actual cache.
 * Caching parameterized plans makes it possible to skip optimization and reuse already optimized plans
 * even if the actual parameters of the query differ from the cached one.
 * This is especially relevant for fast running (TPC-C) queries where the optimization contributes a large
 * share to the overall runtime.

 * Instantiation:
 *                       ┌───> parameterized unoptimized lqp^
 *   unoptimized_lqp ────┤
 *                       └───> value expressions^

 * try get:
 *
 * lqp_cache->try_get(parameterized unoptimized lqp^) ───> parameterized optimized lqp ───┐
 *																						  ├───> optimized lqp
 *          														value expressions^ ───┘

 * set:
 *									value expressions^ ───┐
 *														  ├───> rearrange parameters ───> lqp_cache->set(parameterized optimized lqp)
 *                   ┌───> parameterized optimized lqp ───┘
 * optimized_lqp ────┤
 *                   └───> value expressions (dropped)
 *

 * ^ : member variables

 * The unoptimized_lqp that serves as cache key for the caller is split into a parameterized lqp (all
 * value expressions get replaced by placeholder expressions) and a vector of value expressions.
 * Since the value expressions of the unoptimized_lqp are necessary to cache and instantiate the parameterized
 * optimized plan that has been cached, this class is intended to be newly instantiated for each access
 * with the same key and cannot be reused when accessing the cache with a new key. This is ensured by
 * setting the key (unoptimized lqp) in the constructor.
 */

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
  static const std::tuple<std::shared_ptr<AbstractLQPNode>, std::vector<std::shared_ptr<AbstractExpression>>
  _split_lqp_values(const std::shared_ptr<AbstractLQPNode>& lqp);

  const std::shared_ptr<SQLLogicalPlanCache>& _lqp_cache;
  std::shared_ptr<AbstractLQPNode> _cache_key;
  std::vector<std::shared_ptr<AbstractExpression>> _extracted_values;
  std::chrono::nanoseconds& _cache_duration;
  const UseMvcc _use_mvcc;
};

}  // namespace opossum
