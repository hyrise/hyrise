#pragma once

#include <boost/thread/future.hpp>

#include "scheduler/abstract_task.hpp"

namespace opossum {

// This is the base class for all server-related tasks. Every tasks needs the promise that is defined here. Each custom
// task that is started from within the server application code should inherit from this class.
template <typename T>
class AbstractServerTask : public AbstractTask {
 public:
  AbstractServerTask() = default;

  using result_type = T;

  boost::future<T> get_future() { return _promise.get_future(); }

 protected:
  boost::promise<T> _promise;
};

}  // namespace opossum
