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

    void AbstractLQPGenerator::execute() const {
      auto measurement_export = MeasurementExport("."); //TODO change path_to_dir but remove this from here

      for (const std::shared_ptr<AbstractLQPNode>& lqp : _lqps) {
        const auto pqp = LQPTranslator{}.translate_node(lqp);
        const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
        Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

        measurement_export.export_to_csv(pqp); //TODo Remove this
      }
    }

    const std::vector<std::shared_ptr<AbstractLQPNode>> &AbstractLQPGenerator::get_lqps() const {
      return _lqps;
    }

}
