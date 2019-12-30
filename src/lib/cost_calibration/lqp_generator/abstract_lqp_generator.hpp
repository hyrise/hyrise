#include <vector>
#include <logical_query_plan/abstract_lqp_node.hpp>
#include "storage/table.hpp"

namespace opossum {
  class AbstractLQPGenerator {
    public:
      AbstractLQPGenerator();

      void generate();
      void execute();
      void get();

  protected:
      std::shared_ptr<Table> _table;
      std::vector<std::shared_ptr<AbstractLQPNode>> _lqps;
  };
}
