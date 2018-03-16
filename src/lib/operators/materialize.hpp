#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {

/**
 * @brief Materializes a table
 *
 * Forwards value columns. Turns all other columns into value columns.
 *
 * The operator does not copy the MVCC columns of the input table.
 */
class Materialize : public AbstractReadOnlyOperator {
 public:
  Materialize(const std::shared_ptr<const AbstractOperator> in);

  const std::string name() const final;
  const std::string description(DescriptionMode description_mode) const final;

 protected:
  std::shared_ptr<const Table> _on_execute() final;

  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const final;
};

}  // namespace opossum
