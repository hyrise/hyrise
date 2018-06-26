#include "recover_database_task.hpp"

#include "concurrency/logging/logger.hpp"

namespace opossum {

void RecoverDatabaseTask::_on_execute() {
  try {
    Logger::getInstance().recover();
    _promise.set_value();
  } catch (const std::exception& exception) {
    _promise.set_exception(boost::current_exception());
  }
}

}  // namespace opossum
