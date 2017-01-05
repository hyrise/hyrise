#pragma once

#include <memory>
#include <string>

#include "abstract_operator.hpp"
#include "types.hpp"

namespace opossum {

class Difference : public AbstractOperator {
 public:
  Difference(const std::shared_ptr<const AbstractOperator> left_in,
             const std::shared_ptr<const AbstractOperator> right_in);

  const std::string name() const override;
  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;

 protected:
  void initialize_chunk(const size_t chunk_id);
  std::shared_ptr<const Table> on_execute() override;
};
}  // namespace opossum
