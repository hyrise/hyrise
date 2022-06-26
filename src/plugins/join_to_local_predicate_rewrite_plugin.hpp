#pragma once

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "utils/abstract_plugin.hpp"

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

 protected:
  std::string _table_name;
  ColumnID _column_id;
};

using UCCCandidates = std::vector<UCCCandidate>;

class JoinToLocalPredicateRewritePlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

 protected:
  UCCCandidates* identify_ucc_candidates();
  UCCCandidate* generate_valid_candidate(std::shared_ptr<AbstractLQPNode> root_node, std::shared_ptr<LQPColumnExpression> column_candidate);
};

}  // namespace opossum
