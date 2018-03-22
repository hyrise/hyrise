#pragma once

#include "../../../storage/chunk.hpp"
#include "../../../storage/table.hpp"
#include "jit_abstract_operator.hpp"

namespace opossum {

/**
 * JitAbstractSink is the last jit operator in the operator chain.
 * It is responsible for transforming everything back into the data format the rest
 * of the query pipeline uses. It does this by:
 * 1) adding column definitions to the output table
 * 2) storing the tuples in output chunks
 * 3) appending the output chunks to the output table
 */
class JitAbstractSink : public JitAbstractOperator {
 public:
  virtual ~JitAbstractSink() = default;

  virtual std::shared_ptr<Table> create_output_table(const uint32_t max_chunk_size) const = 0;
  virtual void before_query(Table& out_table, JitRuntimeContext& context) const {}
  virtual void after_query(Table& out_table, JitRuntimeContext& context) const {}

  virtual void after_chunk(Table& out_table, JitRuntimeContext& context) const {}
};

}  // namespace opossum
