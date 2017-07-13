#pragma once

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "product.hpp"
#include "storage/column_visitable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "types.hpp"

namespace opossum {

class SortMergeJoin : public AbstractJoinOperator {
 public:
  SortMergeJoin(const std::shared_ptr<const AbstractOperator> left, const std::shared_ptr<const AbstractOperator> right,
                optional<std::pair<std::string, std::string>> column_names, const std::string& op, const JoinMode mode,
                const std::string& prefix_left, const std::string& prefix_right);

  std::shared_ptr<const Table> on_execute() override { return _impl->on_execute(); };

  const std::string name() const override { return "SortMergeJoin"; };
  uint8_t num_in_tables() const override { return 2u; };
  uint8_t num_out_tables() const override { return 1u; };

 protected:
  template <typename T>
  class SortMergeJoinImpl;

  std::unique_ptr<AbstractJoinOperatorImpl> _impl;
};

}  // namespace opossum
