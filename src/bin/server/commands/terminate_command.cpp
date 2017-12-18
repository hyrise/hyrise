#include "terminate_command.hpp"

#include "../hyrise_session.hpp"

namespace opossum {

void TerminateCommand::start(std::size_t size) {
  // TODO:  After this line runs, both the session and the command is immediately freed.
  // That is not obvious from the API
  _session.terminate_session();
}

} // namespace opossum