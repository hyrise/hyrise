#include "string"
#include <logical_query_plan/abstract_lqp_node.hpp>
#include "cost_calibration/calibration_table_wrapper.hpp"

namespace opossum{
class LQPGenerator {
 public:
  std::vector<std::shared_ptr<AbstractLQPNode>> generate(OperatorType operator_type, std::shared_ptr<const CalibrationTableWrapper> table) const;

 private:
  std::vector<std::shared_ptr<AbstractLQPNode>> _generate_table_scans(const std::shared_ptr<const CalibrationTableWrapper>& table) const;
  void _add_lqps_with_reference_scans(std::vector<std::shared_ptr<AbstractLQPNode>>& list, const double lower_bound_predicate, const double upper_bound, std::shared_ptr<StoredTableNode> table, const std::string& column_name) const;
};
}