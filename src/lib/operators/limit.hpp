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
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override;

  size_t num_rows() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  const size_t _num_rows;
};
}  // namespace opossum
