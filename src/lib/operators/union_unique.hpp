#pragma once

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

class UnionUnique : public AbstractReadOnlyOperator {
 public:
  UnionUnique(const std::shared_ptr<const AbstractOperator> & left,
              const std::shared_ptr<const AbstractOperator> & right);

  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant> &args) const override;
  const std::string name() const override;
  const std::string description() const override;

 private:
  std::shared_ptr<const Table> _on_execute() override;
};

}