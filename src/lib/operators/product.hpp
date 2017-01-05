#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "types.hpp"

namespace opossum {

// Operator to calculate the cartesian product (unconditional join)
// This is for demonstration purposes and for supporting the full relational algebra.
// To avoid ambiguity for instance when doing a self-join, the column names can be prefixed

class Product : public AbstractOperator {
 public:
  Product(const std::shared_ptr<const AbstractOperator> left, const std::shared_ptr<const AbstractOperator> right,
          const std::string prefix_left = "", const std::string prefix_right = "");

  virtual const std::string name() const override;
  virtual uint8_t num_in_tables() const override;
  virtual uint8_t num_out_tables() const override;

 protected:
  void add_product_of_two_chunks(std::shared_ptr<Table> output, ChunkID chunk_id_left, ChunkID chunk_id_right);
  virtual std::shared_ptr<const Table> on_execute() override;

  const std::string _prefix_left, _prefix_right;
};
}  // namespace opossum
