#pragma once

#include <memory>
#include <string>

#include "types.hpp"

#include "sql/sql_plan_cache.hpp"
#include "sql_pipeline.hpp"
#include "sql_pipeline_statement.hpp"

namespace opossum {

class Optimizer;

/**
 * Interface for the configured execution of SQL.
 *
 * Minimal usage:
 *      SQLPipelineBuilder{"SELECT * FROM t;"}.create_pipeline().get_result_table()
 *
 * With custom Optimizer and TransactionContext:
 *      SQLPipelineBuilder{query}.
 *          with_optimizer(optimizer).
 *          with_transaction_context(tc).
 *          create_pipeline();
 *
 * Defaults:
 *  - MVCC is enabled
 *  - The default Optimizer (Optimizer::create_default_optimizer()) is used.
 *
 * Favour this interface over calling the SQLPipeline[Statement] constructors with their long parameter list.
 * See SQLPipeline[Statement] doc for these classes, in short SQLPipeline ist for queries with multiple statement,
 * SQLPipelineStatement for single statement queries.
 */
class SQLPipelineBuilder final {
 public:
  // Plan caches used if `with_{l/p}qp_cache()` are not used in this builder. Both default caches can be nullptr
  // themselves. If both default_{l/p}qp_cache and _{l/p}qp_cache are nullptr, no plan caching is used.
  // These default caches stem from the extended discussion in #1615 and are mainly for Plugins, whose only
  // way of communicating with Hyrise are global variables.
  static std::shared_ptr<SQLPhysicalPlanCache> default_pqp_cache;
  static std::shared_ptr<SQLLogicalPlanCache> default_lqp_cache;

  explicit SQLPipelineBuilder(const std::string& sql);

  SQLPipelineBuilder& with_mvcc(const UseMvcc use_mvcc);
  SQLPipelineBuilder& with_optimizer(const std::shared_ptr<Optimizer>& optimizer);
  SQLPipelineBuilder& with_transaction_context(const std::shared_ptr<TransactionContext>& transaction_context);
  SQLPipelineBuilder& with_pqp_cache(const std::shared_ptr<SQLPhysicalPlanCache>& pqp_cache);
  SQLPipelineBuilder& with_lqp_cache(const std::shared_ptr<SQLLogicalPlanCache>& lqp_cache);

  /**
   * Short for with_mvcc(UseMvcc::No)
   */
  SQLPipelineBuilder& disable_mvcc();

  /*
   * Keep temporary tables in the middle of the query plan for visualization and debugging
   */
  SQLPipelineBuilder& dont_cleanup_temporaries();

  SQLPipeline create_pipeline() const;

  /**
   * @param parsed_sql  for usage from SQLPipeline to pass along to SQLPipelineStatement, everyone else leaves this as
   *                    nullptr
   */
  SQLPipelineStatement create_pipeline_statement(std::shared_ptr<hsql::SQLParserResult> parsed_sql = nullptr) const;

 private:
  const std::string _sql;

  UseMvcc _use_mvcc{UseMvcc::Yes};
  std::shared_ptr<TransactionContext> _transaction_context;
  std::shared_ptr<Optimizer> _optimizer;
  std::shared_ptr<SQLPhysicalPlanCache> _pqp_cache;
  std::shared_ptr<SQLLogicalPlanCache> _lqp_cache;
  CleanupTemporaries _cleanup_temporaries{true};
};

}  // namespace opossum
