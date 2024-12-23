#pragma once

#include <memory>
#include <string>

#include "sql/sql_plan_cache.hpp"
#include "sql_pipeline.hpp"
#include "sql_pipeline_statement.hpp"
#include "types.hpp"

namespace hyrise {

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
 * Favour this interface over calling the SQLPipeline[Statement] constructors with their long parameter list. See
 * SQLPipeline[Statement] doc for these classes. In short, SQLPipeline is for queries with multiple statements,
 * SQLPipelineStatement for single statement queries.
 */
class SQLPipelineBuilder final {
 public:
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

  SQLPipeline create_pipeline() const;

 private:
  const std::string _sql;

  UseMvcc _use_mvcc{UseMvcc::Yes};
  std::shared_ptr<TransactionContext> _transaction_context;
  std::shared_ptr<Optimizer> _optimizer;
  std::shared_ptr<SQLPhysicalPlanCache> _pqp_cache;
  std::shared_ptr<SQLLogicalPlanCache> _lqp_cache;
};

}  // namespace hyrise
