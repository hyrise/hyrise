#pragma once

#include "../../../storage/chunk.hpp"
#include "../../../storage/table.hpp"
#include "abstract_jittable.hpp"

namespace opossum {

/**
 * AbstractJittableSink is the last jit operator in the operator chain.
 * It is responsible for transforming everything back into the data format the rest
 * of the query pipeline uses. It does this by:
 * 1) adding column definitions to the output table
 * 2) storing the tuples in output chunks
 * 3) appending the output chunks to the output table
 */
class AbstractJittableSink : public AbstractJittable {
 public:
  virtual ~AbstractJittableSink() = default;

  // This function is responsible to create an empty output table with appropriate column definitions.
  virtual std::shared_ptr<Table> create_output_table(const Table& in_table) const = 0;

  // This function is called by the JitOperatorWrapper after all operators in the chain have been connected and just
  // before the execution of the pipeline starts (i.e. _execute() is called on the first operator for the first Chunk).
  // It is used for operator initializations.
  virtual void before_query(Table& out_table, JitRuntimeContext& context) const {}

  // This function is called by the JitOperatorWrapper after all chunks have been pushed through the pipeline.
  // It is used for finalizing the output table.
  virtual void after_query(Table& out_table, JitRuntimeContext& context) const {}

  // This function is called by the JitOperatorWrapper after each Chunk that has been pushed through the pipeline.
  // It is used to create a new chunk in the output table for each input chunk.
  virtual void after_chunk(const std::shared_ptr<const Table>& in_table, Table& out_table,
                           JitRuntimeContext& context) const {}
};

}  // namespace opossum
