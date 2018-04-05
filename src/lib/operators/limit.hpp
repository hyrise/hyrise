#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {
// operator to limit the input to n rows
class Limit : public AbstractReadOnlyOperator {
 public:
  explicit Limit(const AbstractOperatorCSPtr in, const size_t num_rows);

  const std::string name() const override;

  size_t num_rows() const;

 protected:
  TableCSPtr _on_execute() override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

 private:
  const size_t _num_rows;
};
}  // namespace opossum
