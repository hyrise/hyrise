#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <atomic>
#include <mutex>

#include "dependency_mining/util.hpp"

namespace opossum {

class DependencyValidator {
 public:
  DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue,
                      tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes,
                      size_t id);

 protected:
  friend class DependencyMiningPlugin;
  void start();
  void stop();

 private:
  bool _validate_od(const DependencyCandidate& candidate, std::ostream& out);
  bool _validate_fd(const DependencyCandidate& candidate, std::ostream& out);
  bool _validate_ucc(const DependencyCandidate& candidate, std::ostream& out);
  bool _validate_ind(const DependencyCandidate& candidate, std::ostream& out);
  const std::shared_ptr<DependencyCandidateQueue>& _queue;
  std::atomic_bool _running = false;
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& _table_constraint_mutexes;
  const size_t _id;
};

}  // namespace opossum
