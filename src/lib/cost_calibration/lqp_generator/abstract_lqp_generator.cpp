#include <logical_query_plan/lqp_translator.hpp>
#include <scheduler/operator_task.hpp>
#include <logical_query_plan/projection_node.hpp>
#include <cost_calibration/calibration_table_wrapper.hpp>
#include "table_scan.hpp"
#include "hyrise.hpp"


namespace opossum {

    AbstractLQPGenerator::AbstractLQPGenerator(const std::shared_ptr<const CalibrationTableWrapper>& table) {
      _lqps = std::vector<std::shared_ptr<AbstractLQPNode>>();
      _table = table;
    }

    void AbstractLQPGenerator::execute() const {
      for (const std::shared_ptr<AbstractLQPNode>& lqp : _lqps) {
        const auto pqp = LQPTranslator{}.translate_node(lqp);
        const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
        Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
      }
    }

    std::vector<std::shared_ptr<AbstractLQPNode>> AbstractLQPGenerator::get() const {
      return _lqps;
    }

}
