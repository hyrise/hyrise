#include "table_scan.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "expression/expression_functional.hpp"

namespace opossum {
    using namespace opossum::expression_functional;
    void TableScanLQPGenerator::generate() {

      int selectivity_steps = 10;

      // TODO actually implement this
      Hyrise::get().storage_manager.add_table("t_a", _table->getTable());
      const auto _t_a = StoredTableNode::make("t_a");
      //const auto _t_a_a = StaticTableNode::make(table);

      const auto _a = _t_a->get_column("_a");
      const auto _b = _t_a->get_column("_b");

      for (int i = 0; i < selectivity_steps; i++) {
        const auto _projection_node_a =
                ProjectionNode::make(expression_vector(_a),
                                     PredicateNode::make(greater_than_(_b, 10 * selectivity_steps), _t_a));
        _lqps.emplace_back(_projection_node_a);
      }
    }

}
