#pragma once

#include "abstract_server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class PreparedPlan;

class ParseServerPreparedStatementTask : public AbstractServerTask<std::unique_ptr<PreparedPlan>> {
 public:
  explicit ParseServerPreparedStatementTask(const std::string& query) : _query(query) {}

 protected:
  void _on_execute() override;

  std::string _query;
};

}  // namespace opossum
