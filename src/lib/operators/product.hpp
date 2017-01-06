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
  void execute() override;
  std::shared_ptr<const Table> get_output() const override;

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  void add_product_of_two_chunks(ChunkID chunk_id_left, ChunkID chunk_id_right);

  const std::shared_ptr<Table> _table;

  const std::string _prefix_left, _prefix_right;

  std::shared_ptr<Table> _output;
};
}  // namespace opossum
