#include "dependency_validator.hpp"

#include <magic_enum.hpp>

#include "utils/timer.hpp"
#include "validation_strategy/fd_validation_rule.hpp"
#include "validation_strategy/ind_validation_rule.hpp"
#include "validation_strategy/od_validation_rule.hpp"
#include "validation_strategy/ucc_validation_rule.hpp"

namespace opossum {

DependencyValidator::DependencyValidator(
    const std::shared_ptr<DependencyCandidateQueue>& queue,
    tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes, size_t id)
    : _queue(queue), _id(id) {
  add_rule(std::make_unique<ODValidationRule>(table_constraint_mutexes));
  add_rule(std::make_unique<UCCValidationRule>(table_constraint_mutexes));
  add_rule(std::make_unique<FDValidationRule>(table_constraint_mutexes));
  add_rule(std::make_unique<INDValidationRule>(table_constraint_mutexes));
}

void DependencyValidator::start() {
  _running = true;
  std::cout << "Run DependencyValidator " + std::to_string(_id) + "\n";
  Timer timer;
  DependencyCandidate candidate;
  while (_queue->try_pop(candidate)) {
    Timer candidate_timer;
    //std::stringstream my_out;
    std::cout << "[" << _id << "] Check candidate: " << candidate << std::endl;
    const auto status = _rules[candidate.type]->is_valid(candidate);
    std::cout << "    " << magic_enum::enum_name(status) << "    " << candidate_timer.lap_formatted() << std::endl;
    //std::cout << my_out.rdbuf();
  }
  std::cout << "DependencyValidator " + std::to_string(_id) + " finished in " + timer.lap_formatted() + "\n";
}

void DependencyValidator::stop() { _running = false; }

void DependencyValidator::add_rule(std::unique_ptr<AbstractDependencyValidationRule> rule) {
  _rules.emplace(rule->dependency_type, std::move(rule));
}

}  // namespace opossum
