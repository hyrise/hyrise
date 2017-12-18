#pragma once

#include "abstract_command.hpp"

namespace opossum {

class TerminateCommand : public AbstractCommand {
 public:
  explicit TerminateCommand(HyriseSession &session)
          : AbstractCommand(session) {}

  void start(std::size_t size) override;

};

}  // namespace opossum