#include "abstract_lqp_generator.hpp"

namespace opossum {
    class TableScanLQPGenerator : public AbstractLQPGenerator {
    public:
        TableScanLQPGenerator();
          void generate();
          void execute();
          void get();
    };
}
