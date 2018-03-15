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
 */
class Materialize : public AbstractReadOnlyOperator {
 public:
  Materialize(const std::shared_ptr<const AbstractOperator> in);

  const std::string name() const final;
  const std::string description(DescriptionMode description_mode) const final;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args = {}) const final;

 protected:
  std::shared_ptr<const Table> _on_execute() final;
};

}  // namespace opossum
