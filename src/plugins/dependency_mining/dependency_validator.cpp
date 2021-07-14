#include "dependency_validator.hpp"

#include "magic_enum.hpp"

namespace opossum {

// void DependencyValidator::set_queue(const DependencyCandidateQueue& queue) { _queue = queue; };

DependencyValidator::DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue) : _queue(queue) {}

void DependencyValidator::start() {
  _running = true;
  std::cout << "Run DependencyValidator" << std::endl;
  DependencyCandidate current_item;
  while (_queue->try_pop(current_item)) {
    std::cout << "Check " << magic_enum::enum_name(current_item.type) << " candidate with prio "
              << current_item.priority << std::endl;
  }
}

void DependencyValidator::stop() { _running = false; }

}  // namespace opossum
