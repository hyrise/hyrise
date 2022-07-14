#include "plan_exporter.hpp"

namespace opossum {

void PlanExporter::add_plan(const std::string& hash, const std::shared_ptr<const AbstractOperator>& pqp) {
  Assert(!_pqps.contains(hash), "Query with same hash seen before");
  _pqps[hash] = pqp;
}

void PlanExporter::export_plans(const std::string& file_name) {
  std::cout << "export plans to " << file_name << std::endl;
}

}  // namespace opossum
