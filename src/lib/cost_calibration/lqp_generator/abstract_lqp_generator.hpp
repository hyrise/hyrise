#include <vector>
#include <logical_query_plan/abstract_lqp_node.hpp>
#include "storage/table.hpp"

namespace opossum {
  class AbstractLQPGenerator {
    public:
      AbstractLQPGenerator();

      virtual void generate() = 0;
      void execute();
      void get();

  protected:
      std::shared_ptr<Table> _table;
      std::vector<std::shared_ptr<AbstractLQPNode>> _lqps;
  };
}
