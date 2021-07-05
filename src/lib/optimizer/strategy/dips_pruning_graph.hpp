#pragma once

#include <set>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "resolve_type.hpp"

namespace opossum {
struct DipsPruningGraph {
  friend class DipsPruningRuleTest_BuildJoinGraph_Test;
  friend class DipsPruningRuleTest_JoinGraphIsTree_Test;
  friend class DipsPruningRuleTest_DipsJoinGraphIsNoTree_Test;
  friend class DipsPruningRuleTest_DipsJoinGraphTraversal_Test;

  struct JoinGraphEdge {
   public:

    explicit JoinGraphEdge(std::set<size_t> init_vertex_set, std::shared_ptr<BinaryPredicateExpression> predicate)  : vertex_set(init_vertex_set) {
       predicates.push_back(predicate);
    }

    void append_predicate(std::shared_ptr<BinaryPredicateExpression> predicate) {
      // TODO(somebody): remove search when implementation "visit single node in LQP only once" is done
      if (std::find(predicates.begin(), predicates.end(), predicate) == predicates.end()) {
        predicates.push_back(predicate);
      }
    }

    bool connects_vertex(size_t vertex) {
      return vertex_set.find(vertex) != vertex_set.end();
    }

    size_t neighbour(size_t vertex) {
      for(auto neighbour : vertex_set) {
        if (neighbour != vertex) {
          return neighbour;
        }
      }
      Assert(false, "There always should be a neighbor");
    }

    std::set<size_t> vertex_set;
    std::vector<std::shared_ptr<BinaryPredicateExpression>> predicates;
  };

void build_graph(const std::shared_ptr<AbstractLQPNode>& node);
std::vector<JoinGraphEdge> top_down_traversal();
std::vector<JoinGraphEdge> bottom_up_traversal();
bool is_tree();
bool empty();

private:
  size_t _get_vertex(std::shared_ptr<StoredTableNode> table_node);
  std::set<size_t> _get_vertex_set(size_t noda_a, size_t noda_b);
  void _add_edge(std::set<size_t> vertex_set, std::shared_ptr<BinaryPredicateExpression> predicate);
  bool _is_tree_visit(size_t current_node, size_t parrent, std::set<size_t>& visited);
  void _top_down_traversal_visit(size_t current_node, std::vector<JoinGraphEdge>& traversal_order, std::set<size_t>& visited);
  void _bottom_up_traversal_visit(size_t current_node, std::vector<JoinGraphEdge>& traversal_order, std::set<size_t>& visited);


  std::vector<JoinMode> supported_join_types{JoinMode::Inner, JoinMode::Semi};
  std::vector<std::shared_ptr<StoredTableNode>> vertices;
  std::vector<JoinGraphEdge> edges;
};

} // namespace opossum
