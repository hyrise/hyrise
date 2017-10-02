#pragma once

#include "scheduler/abstract_task.hpp"

namespace opossum {

class MigrationPreparationTask : public AbstractTask {
 public:
  MigrationPreparationTask();

 protected:
  void _on_execute() override;
};
}
