#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator to calculate the cartesian product (unconditional join)
 * This is for demonstration purposes and for supporting the full relational algebra.
 *
 * Note: Product does not support null values at the moment
 */
class Product : public AbstractReadOnlyOperator {
 public:
  Product(const std::shared_ptr<const AbstractOperator> left, const std::shared_ptr<const AbstractOperator> right);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override {
    Fail("Operator " + this->name() + " does not implement recreation.");
    return {};
  }

 protected:
  void add_product_of_two_chunks(std::shared_ptr<Table> output, ChunkID chunk_id_left, ChunkID chunk_id_right);
  std::shared_ptr<const Table> _on_execute() override;
};
}  // namespace opossum
