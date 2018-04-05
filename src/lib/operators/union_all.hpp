#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class UnionAll : public AbstractReadOnlyOperator {
 public:
  UnionAll(const AbstractOperatorCSPtr left_in,
           const AbstractOperatorCSPtr right_in);
  const std::string name() const override;

 protected:
  TableCSPtr _on_execute() override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

  TableSPtr _output;
};
}  // namespace opossum
