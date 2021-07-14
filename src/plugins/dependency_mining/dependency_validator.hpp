#pragma once

#include <atomic>

#include "dependency_mining/util.hpp"

namespace opossum {

class DependencyValidator {
 public:
  DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue);

 protected:
  friend class DependencyMiningPlugin;
  // void set_queue(const DependencyCandidateQueue& queue);
  void start();
  void stop();

 private:
  const std::shared_ptr<DependencyCandidateQueue>& _queue;
  std::atomic_bool _running = false;
};

}  // namespace opossum
