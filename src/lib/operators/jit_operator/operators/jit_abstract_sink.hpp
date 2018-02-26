#pragma once

#include "../../../storage/chunk.hpp"
#include "../../../storage/table.hpp"
#include "jit_abstract_operator.hpp"

namespace opossum {

/**
 * JitAbstractSink is the last jit operator in the operator chain.
 * It is responsible for:
 * 1) adding column definitions to the output table
 * 2) storing the tuples in output chunks
 * 3) appending the output chunks to the output table
 *
 * In order to it has a number of virtual methods, that are called before the query
 * is executed and after processing each input chunk is completed.
 */
class JitAbstractSink : public JitAbstractOperator {
 public:
  virtual ~JitAbstractSink() = default;

  virtual void before_query(Table& out_table, JitRuntimeContext& ctx) {}
  virtual void after_chunk(Table& out_table, JitRuntimeContext& ctx) const {}
};

}  // namespace opossum
