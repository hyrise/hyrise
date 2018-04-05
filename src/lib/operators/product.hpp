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
  Product(const AbstractOperatorCSPtr left, const AbstractOperatorCSPtr right);

  const std::string name() const override;

 protected:
  void add_product_of_two_chunks(TableSPtr output, ChunkID chunk_id_left, ChunkID chunk_id_right);
  TableCSPtr _on_execute() override;
  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;
};
}  // namespace opossum
