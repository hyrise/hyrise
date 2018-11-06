#pragma once

#include "abstract_server_task.hpp"

#include "all_parameter_variant.hpp"

namespace opossum {

class LQPPreparedStatement;

class ParseServerPreparedStatementTask : public AbstractServerTask<std::unique_ptr<LQPPreparedStatement>> {
 public:
  explicit ParseServerPreparedStatementTask(const std::string& query)
      : _query(query) {}

 protected:
  void _on_execute() override;

  std::string _query;
};

}  // namespace opossum
