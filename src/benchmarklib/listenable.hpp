#pragma once

#include <json.hpp>

#include <any>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace opossum {

using EventListener = std::function<void(std::any)>;

template <class EventType>
class Listenable {
 public:
  virtual ~Listenable() {}
  virtual void add_listener(const EventType event, EventListener listener) {
    if (_event_listeners.find(event) == _event_listeners.end()) {
      _event_listeners.emplace(event, std::vector<EventListener>{listener});
    } else {
      _event_listeners[event].push_back(listener);
    }
  }

 protected:
  virtual void _notify_listeners(const EventType event, std::any payload) const {
    auto it = _event_listeners.find(event);
    if (it != _event_listeners.end()) {
      for (auto listener : it->second) {
        listener(payload);
      }
    }
  }

  std::unordered_map<EventType, std::vector<EventListener>> _event_listeners;
};

}  // namespace opossum
