#include <vector>
#include <logical_query_plan/abstract_lqp_node.hpp>
#include "storage/table.hpp"

namespace opossum {
  class AbstractLQPGenerator {
    public:
      // TODO check if const etc is correct
      explicit AbstractLQPGenerator(const std::shared_ptr<const CalibrationTableWrapper> &table);

      virtual void generate() = 0;
      void execute() const;
      [[nodiscard]] std::vector<std::shared_ptr<AbstractLQPNode>> get() const;

  protected:
      std::shared_ptr<const CalibrationTableWrapper> _table;
      std::vector<std::shared_ptr<AbstractLQPNode>> _lqps;
  };
}
