#include <logical_query_plan/lqp_translator.hpp>
#include <scheduler/operator_task.hpp>
#include <logical_query_plan/projection_node.hpp>
#include <cost_calibration/calibration_table_wrapper.hpp>
#include <cost_calibration/measurement_export.hpp>
#include "table_scan.hpp"
#include "hyrise.hpp"


namespace opossum {

    AbstractLQPGenerator::AbstractLQPGenerator(std::shared_ptr<const CalibrationTableWrapper> table) {
      _lqps = std::vector<std::shared_ptr<AbstractLQPNode>>();
      _table = table;
    }

    const std::vector<std::shared_ptr<AbstractLQPNode>> &AbstractLQPGenerator::get_lqps() const {
      return _lqps;
    }

}
