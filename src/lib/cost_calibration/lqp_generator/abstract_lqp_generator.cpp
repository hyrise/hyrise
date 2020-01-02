#include <logical_query_plan/lqp_translator.hpp>
#include <scheduler/operator_task.hpp>
#include <logical_query_plan/projection_node.hpp>
#include "table_scan.hpp"
#include "hyrise.hpp"


namespace opossum {
    void AbstractLQPGenerator::execute() {
      for (const std::shared_ptr<AbstractLQPNode>& lqp : _lqps) {
        const auto pqp = LQPTranslator{}.translate_node(lqp);
        const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
        // TODO check if system should wait for entire execution after every run
        Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
      }
    }

    void AbstractLQPGenerator::get() {
      // TODO check with CSV generation on preferred return format
    }

    AbstractLQPGenerator::AbstractLQPGenerator() {
      _lqps = std::vector<std::shared_ptr<AbstractLQPNode>>();
    }
}
