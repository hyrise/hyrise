#include <cost_calibration/calibration_table_wrapper.hpp>
#include "abstract_lqp_generator.hpp"

namespace opossum {
    class TableScanLQPGenerator : public AbstractLQPGenerator {
    public:
        using AbstractLQPGenerator::AbstractLQPGenerator;
        void generate() override;
    };
}
