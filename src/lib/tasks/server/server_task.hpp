#pragma once

#include <boost/thread/future.hpp>

#include "scheduler/abstract_task.hpp"

namespace opossum {

template<typename T>
class ServerTask : public AbstractTask {
 public:
  explicit ServerTask(){}
  
  using result_type = T;
  
  boost::future<T> get_future() { return _promise.get_future(); }

 protected:
  boost::promise<T> _promise;
};

}  // namespace opossum
