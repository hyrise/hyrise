#include <vector>
#include <logical_query_plan/abstract_lqp_node.hpp>
#include "storage/table.hpp"

namespace opossum {
  class AbstractLQPGenerator {
    public:
      // TODO check if const etc is correct
      explicit AbstractLQPGenerator(std::shared_ptr<const CalibrationTableWrapper> table);

      void generate();
      void execute() const;

      const std::vector<std::shared_ptr<AbstractLQPNode>>& get_lqps() const;

  protected:
      std::shared_ptr<const CalibrationTableWrapper> _table;
      std::vector<std::shared_ptr<AbstractLQPNode>> _lqps;
  };
}
