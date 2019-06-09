#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

namespace opossum {

enum class Event {
  ItemRunStarted,
  ItemRunFinished,
  CreateReport,
};

using EventListener = std::function<void ()>;

class Listenable {
 public:
  void add_listener(const Event event, EventListener listener);

 protected:
  void _notify_listeners(const Event event) const;

  std::unordered_map<Event, std::vector<EventListener>> _event_listeners;
};

}  // namespace opossum
