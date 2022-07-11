#pragma once

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

class UCCCandidate {
 public:
  UCCCandidate(std::string table_name, ColumnID column_id) : _table_name(table_name), _column_id(column_id)
  {}
  
  const std::string table_name() const {
    return this->_table_name;
  }

  const ColumnID column_id() const {
    return this->_column_id;
  }

  friend bool operator==(const UCCCandidate& lhs, const UCCCandidate& rhs) {
    return (lhs.column_id() == rhs.column_id()) && (lhs.table_name() == rhs.table_name());

  }

 protected:
  std::string _table_name;
  ColumnID _column_id;
};

using UCCCandidates = std::unordered_set<UCCCandidate>;

class JoinToLocalPredicateRewritePlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

  constexpr static std::chrono::milliseconds IDLE_DELAY_PREDICATE_REWRITE = std::chrono::milliseconds(5000);

 protected:
  UCCCandidates* identify_ucc_candidates();
  UCCCandidate* generate_valid_candidate(std::shared_ptr<AbstractLQPNode> root_node, std::shared_ptr<LQPColumnExpression> column_candidate);

  void _start();

  std::unique_ptr<PausableLoopThread> _loop_thread_start;
};

}  // namespace opossum


template<>
struct std::hash<opossum::UCCCandidate>
{
    std::size_t operator()(opossum::UCCCandidate const& s) const noexcept
    {
        std::size_t h1 = std::hash<std::string>{}(s.table_name());
        std::size_t h2 = std::hash<opossum::ColumnID>{}(s.column_id());
        return h1 ^ (h2 << 1);
    }
};