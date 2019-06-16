#pragma once

#include <json.hpp>

#include <any>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace opossum {

enum class Event {
  ItemRunStarted,
  ItemRunFinished,
  CreateReport,
};

using EventListener = std::function<void(std::any)>;

class Listenable {
 public:
  virtual ~Listenable() {}
  virtual void add_listener(const Event event, EventListener listener);

 protected:
  virtual void _notify_listeners(const Event event, std::any payload = std::any()) const;

  std::unordered_map<Event, std::vector<EventListener>> _event_listeners;
};

}  // namespace opossum
