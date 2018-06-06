#pragma once

#include <memory>
#include <string>
#include <vector>

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

class View;

// maintenance operator for the "CREATE VIEW" sql statement
class CreateView : public AbstractReadOnlyOperator {
 public:
  CreateView(const std::string& view_name, const std::shared_ptr<View>& view);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_recreate(
      const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
      const std::shared_ptr<AbstractOperator>& recreated_input_right) const override;

 private:
  const std::string _view_name;
  const std::shared_ptr<View> _view;
};
}  // namespace opossum
