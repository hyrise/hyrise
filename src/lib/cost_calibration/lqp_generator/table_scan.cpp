#include "table_scan.hpp"

#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include <expression/expression_functional.hpp>

namespace opossum {
    void TableScanLQPGenerator::generate() {
      // TODO actually implement this
      Hyrise::get().storage_manager.add_table("t_a", _table);
        const auto _t_a = StoredTableNode::make("t_a");
        //const auto _t_a_a = StaticTableNode::make(table);

        const auto _a = _t_a->get_column("_a");
        const auto _b = _t_a->get_column("_b");

//        _lqps.push_back(
//                std::make_shared<ProjectionNode>(
//                ProjectionNode::make(expression_functional::expression_vector(_a),
//                                     PredicateNode::make(boost::iterator_range_detail::greater_than(_b, 10), _t_a))
//                                     )
//        );
    }

    TableScanLQPGenerator::TableScanLQPGenerator() {

    }
}
