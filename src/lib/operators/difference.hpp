#pragma once

#include <memory>
#include <sstream>
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
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args = {}) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  void initialize_chunk(const size_t chunk_id);
  void append_string_representation(std::ostream& row_string_buffer, const AllTypeVariant value);
};
}  // namespace opossum
