#include "listenable.hpp"

namespace opossum {

void Listenable::add_listener(const Event event, EventListener listener) {
  if (_event_listeners.find(event) == _event_listeners.end()) {
    _event_listeners.emplace(event, std::vector<EventListener>{listener});
  } else {
    _event_listeners[event].push_back(listener);
  }
}

void Listenable::_notify_listeners(const Event event, const nlohmann::json& payload) const {
  auto it = _event_listeners.find(event);
  if (it != _event_listeners.end()) {
    for (auto listener : it->second) {
      listener(payload);
    }
  }
}

}  // namespace opossum
