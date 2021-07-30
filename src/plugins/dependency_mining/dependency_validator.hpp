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
  void _validate_od(const DependencyCandidate& candidate);
  void _validate_fd(const DependencyCandidate& candidate);
  void _validate_ucc(const DependencyCandidate& candidate);
  void _validate_ind(const DependencyCandidate& candidate);
  const std::shared_ptr<DependencyCandidateQueue>& _queue;
  std::atomic_bool _running = false;
};

}  // namespace opossum
