#pragma once

#include <memory>
#include <string>

namespace opossum {

class Topology;

// See Server::start()
struct ServerConfiguration {
  std::string address;
  size_t num_listener_threads;
  bool skip_scheduler;
  std::shared_ptr<Topology> topology;
};

}  // namespace opossum
