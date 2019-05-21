#pragma once

#include <memory>
#include <string>
#include <vector>

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

class LQPView;

// maintenance operator for the "CREATE VIEW" sql statement
class CreateView : public AbstractReadOnlyOperator {
 public:
  CreateView(const std::string& view_name, const std::shared_ptr<LQPView>& view, bool if_not_exists);

  const std::string name() const override;

  const std::string& view_name() const;
  bool if_not_exists() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  const std::string _view_name;
  const std::shared_ptr<LQPView> _view;
  const bool _if_not_exists;
};
}  // namespace opossum
