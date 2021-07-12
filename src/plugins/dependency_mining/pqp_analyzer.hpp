#pragma once

#include "dependency_mining/util.hpp"

namespace opossum {

class PQPAnalyzer : public Noncopyable {
 protected:
  friend class DependencyMiningPlugin;
  void set_queue(const DependencyCandidateQueue& queue);
  void run();

 private:
  DependencyCandidateQueue _queue;
};

}  // namespace opossum
