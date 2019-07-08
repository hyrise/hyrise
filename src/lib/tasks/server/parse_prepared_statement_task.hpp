#pragma once

#include "abstract_server_task.hpp"

namespace opossum {

class PreparedPlan;

class ParsePreparedStatementTask : public AbstractTask {
 public:
  explicit ParsePreparedStatementTask(const std::string& query) : _query(query) {}

  std::unique_ptr<PreparedPlan> get_plan();

 protected:
  void _on_execute() override;

 private:
  std::unique_ptr<PreparedPlan> _prepared_plan;
  std::string _query;
};

}  // namespace opossum
