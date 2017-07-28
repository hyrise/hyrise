#pragma once

#include <map>
#include <memory>
#include <vector>

#include "opossum.pb.wrapper.hpp"

namespace opossum {

class OperatorTask;

// Translates a Protocol Buffer object tree structure into OperatorTasks with dependencies
class OperatorTranslator {
 public:
  // Recursively creates Tasks for all input-operators of `op` and a task for `op` itself
  const std::vector<std::shared_ptr<OperatorTask>>& build_tasks_from_proto(const proto::OperatorVariant& op);
  // Returns the root task, i.e. the root element of the dependency tree structure. It is the last one to be executed by
  // the scheduler.
  std::shared_ptr<OperatorTask> root_task() { return _root_task; }

 protected:
  std::shared_ptr<OperatorTask> translate_proto(const proto::OperatorVariant& op);
  inline std::shared_ptr<OperatorTask> translate(const proto::GetTableOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::TableScanOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::ProjectionOperator& projection_operator);
  inline std::shared_ptr<OperatorTask> translate(const proto::ProductOperator& product_operator);
  inline std::shared_ptr<OperatorTask> translate(const proto::SortOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::UnionAllOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::ImportCsvOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::PrintOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::DifferenceOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::ExportCsvOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::ExportBinaryOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::IndexColumnScanOperator&);
  inline std::shared_ptr<OperatorTask> translate(const proto::NestedLoopJoinOperator&);
  std::vector<std::shared_ptr<OperatorTask>> _tasks;
  std::shared_ptr<OperatorTask> _root_task;
};

}  // namespace opossum
