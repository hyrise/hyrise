#include "terminate_command.hpp"

#include <server/hyrise_session.hpp>

namespace opossum {

void TerminateCommand::start(std::size_t size) {
  // TODO(rs22):  After this line runs, both the session and the command is immediately freed.
  // That is not obvious from the API
  _session.terminate_session();
}

}  // namespace opossum
