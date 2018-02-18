#pragma once

#include <string>

#include "abstract_read_only_operator.hpp"
#include "jit_operator/operators/jit_abstract_sink.hpp"
#include "operators/jit_operator/operators/jit_read_table.hpp"

namespace opossum {

class JitOperator : public AbstractReadOnlyOperator {
 public:
  explicit JitOperator(const std::shared_ptr<const AbstractOperator> left, const bool use_jit = false);

  const std::string name() const final;
  const std::string description(DescriptionMode description_mode) const final;

  void add_jit_operator(const JitAbstractOperator::Ptr& op);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  const JitReadTable::Ptr _source() const;
  const JitAbstractSink::Ptr _sink() const;

  const bool _use_jit;
  std::vector<JitAbstractOperator::Ptr> _operators;
};

}  // namespace opossum
