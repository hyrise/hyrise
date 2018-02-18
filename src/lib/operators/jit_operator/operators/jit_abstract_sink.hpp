#pragma once

#include "../../../storage/chunk.hpp"
#include "../../../storage/table.hpp"
#include "jit_abstract_operator.hpp"

namespace opossum {

class JitAbstractSink : public JitAbstractOperator {
 public:
  using Ptr = std::shared_ptr<JitAbstractSink>;

  virtual ~JitAbstractSink() = default;

  virtual void before_query(Table& out_table, JitRuntimeContext& ctx) {}
  virtual void after_chunk(Table& out_table, JitRuntimeContext& ctx) const {}
};

}  // namespace opossum
