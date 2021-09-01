#include "dependency_validator.hpp"

#include <magic_enum.hpp>

#include "hyrise.hpp"
#include "utils/timer.hpp"
#include "validation_strategy/fd_validation_rule.hpp"
#include "validation_strategy/ind_validation_rule.hpp"
#include "validation_strategy/od_validation_rule.hpp"
#include "validation_strategy/ucc_validation_rule.hpp"

namespace opossum {

DependencyValidator::DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue, size_t id)
    : _queue(queue), _id(id) {
  add_rule(std::make_unique<ODValidationRule>());
  add_rule(std::make_unique<UCCValidationRule>());
  add_rule(std::make_unique<FDValidationRule>());
  add_rule(std::make_unique<INDValidationRule>());
}

void DependencyValidator::start() {
  _running = true;
  std::cout << "Run DependencyValidator " + std::to_string(_id) + "\n";
  Timer timer;
  DependencyCandidate candidate;
  while (_queue->try_pop(candidate)) {
    Timer candidate_timer;
    std::stringstream my_out;
    my_out << "[" << _id << "] Check candidate: " << candidate << std::endl;
    const auto validate_result = _rules[candidate.type]->validate(candidate);
    if (validate_result->status == DependencyValidationStatus::Valid) {
      for (const auto& [table_name, constraints] : validate_result->constraints) {
        _add_constraints(table_name, constraints);
      }
    }
    my_out << "    " << magic_enum::enum_name(validate_result->status) << "    " << candidate_timer.lap_formatted()
           << std::endl;
    std::cout << my_out.rdbuf();
  }
  std::cout << "DependencyValidator " + std::to_string(_id) + " finished in " + timer.lap_formatted() + "\n";
}

void DependencyValidator::stop() { _running = false; }

void DependencyValidator::add_rule(std::unique_ptr<AbstractDependencyValidationRule> rule) {
  _rules.emplace(rule->dependency_type, std::move(rule));
}

void DependencyValidator::_add_constraints(
    const std::string& table_name, const std::vector<std::shared_ptr<AbstractTableConstraint>>& constraints) const {
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  for (const auto& constraint : constraints) {
    switch (constraint->type()) {
      case TableConstraintType::Key:
        table->add_soft_key_constraint(dynamic_cast<const TableKeyConstraint&>(*constraint));
        break;
      case TableConstraintType::Order:
        table->add_soft_order_constraint(dynamic_cast<const TableOrderConstraint&>(*constraint));
        break;
    }
  }
}

}  // namespace opossum
