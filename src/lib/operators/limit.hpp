#pragma once

#include <memory>
#include <string>

#include "abstract_read_only_operator.hpp"

namespace opossum {
// operator to limit the input to n rows
class Limit : public AbstractReadOnlyOperator {
 public:
  explicit Limit(const std::shared_ptr<const AbstractOperator> in, size_t num_rows);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  std::shared_ptr<const Table> on_execute() override;

 private:
  size_t _num_rows;
};
}  // namespace opossum
