#pragma once

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace hyrise {

class UCCCandidate {
 public:
  UCCCandidate(const std::string& table_name, const ColumnID column_id)
      : _table_name(table_name), _column_id(column_id) {}

  const std::string& table_name() const {
    return _table_name;
  }

  const ColumnID column_id() const {
    return _column_id;
  }

  friend bool operator==(const UCCCandidate& lhs, const UCCCandidate& rhs) {
    return (lhs.column_id() == rhs.column_id()) && (lhs.table_name() == rhs.table_name());
  }

 protected:
  const std::string _table_name;
  const ColumnID _column_id;
};

using UCCCandidates = std::unordered_set<UCCCandidate>;

class UccDiscoveryPlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

 protected:
  friend class UccDiscoveryPluginTest;

  UCCCandidates identify_ucc_candidates() const;
  std::shared_ptr<std::vector<UCCCandidate>> generate_valid_candidates(
      std::shared_ptr<AbstractLQPNode> root_node, std::shared_ptr<LQPColumnExpression> column_candidate) const;

  void discover_uccs() const;

  std::unique_ptr<PausableLoopThread> _loop_thread_start;
};

}  // namespace hyrise

template <>
struct std::hash<hyrise::UCCCandidate> {
  std::size_t operator()(const hyrise::UCCCandidate& s) const noexcept {
    std::size_t h1 = std::hash<std::string>{}(s.table_name());
    std::size_t h2 = std::hash<hyrise::ColumnID>{}(s.column_id());
    return h1 ^ (h2 << 1);
  }
};
