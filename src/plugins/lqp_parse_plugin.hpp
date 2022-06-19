#pragma once

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "utils/abstract_plugin.hpp"

namespace opossum {

class UCCCandidate {
 public:
  UCCCandidate(std::string table_name, ColumnID column_id) : _table_name(table_name), _column_id(column_id)
  {}
  
 protected:
  std::string _table_name;
  ColumnID _column_id;
};

    
class LQPParsePlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

protected:
  UCCCandidate* generate_valid_candidate(std::shared_ptr<AbstractLQPNode> node, std::shared_ptr<AbstractExpression> column, bool validated);
};

using UCCCandidates = std::vector<UCCCandidate>;

}  // namespace opossum
