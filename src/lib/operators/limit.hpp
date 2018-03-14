#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {
// operator to limit the input to n rows
class Limit : public AbstractReadOnlyOperator {
 public:
  explicit Limit(const std::shared_ptr<const AbstractOperator> in, const size_t num_rows);

  const std::string name() const override;

  size_t num_rows() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

 private:
  const size_t _num_rows;
};
}  // namespace opossum
