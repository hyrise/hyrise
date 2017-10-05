#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Note: Difference does not support null values at the moment
 */
class Difference : public AbstractReadOnlyOperator {
 public:
  Difference(const std::shared_ptr<const AbstractOperator> left_in,
             const std::shared_ptr<const AbstractOperator> right_in);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

 protected:
  void initialize_chunk(const size_t chunk_id);
  std::shared_ptr<const Table> _on_execute() override;
};
}  // namespace opossum
