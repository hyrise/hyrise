#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/thread/future.hpp>

#include <memory>

#include "hyrise.hpp"
#include "tasks/server/abstract_server_task.hpp"
#include "then_operator.hpp"
#include "use_boost_future.hpp"

namespace opossum {

// This class encapsulates the io_service and thus allows the ServerSession
// to be easily tested with a mocked version of this class.
class TaskRunner {
 public:
  explicit TaskRunner(boost::asio::io_service& io_service) : _io_service(io_service) {}

  template <typename TResult>
  auto dispatch_server_task(std::shared_ptr<TResult> task) -> decltype(task->get_future());

 protected:
  boost::asio::io_service& _io_service;
};

template <typename TResult>
auto TaskRunner::dispatch_server_task(std::shared_ptr<TResult> task) -> decltype(task->get_future()) {
  using opossum::then_operator::then;
  using TaskList = std::vector<std::shared_ptr<AbstractTask>>;

  Hyrise::get().scheduler()->schedule_tasks(TaskList({task}));

  return task->get_future()
      .then(boost::launch::sync,
            [this](auto result) {
              // This result comes in on the scheduler thread, so we want to dispatch it back to the io_service
              return _io_service.post(boost::asio::use_boost_future)
                     // Make sure to be on the main thread before re-throwing the exceptions
                     >> then >> [result = std::move(result)]() mutable { return result.get(); };
            })
      .unwrap();
}

}  // namespace opossum
