#pragma once

#include "operators/abstract_operator.hpp"

namespace opossum {

class PlanExporter {
 public:
  void add_plan(const std::string& hash, const std::shared_ptr<const AbstractOperator>& pqp);

  void export_plans(const std::string& file_name);

 protected:
  std::unordered_map<std::string, std::shared_ptr<const AbstractOperator>> _pqps;
};

}  // namespace opossum
