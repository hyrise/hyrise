#include <sstream>

#include "base_test.hpp"
#include "operators/operator_performance_data.hpp"

namespace opossum {

class OperatorsOperatorPerformanceDataTest : public BaseTest {
 public:
  void SetUp() override {}
};

TEST_F(OperatorsOperatorPerformanceDataTest, OutputToStream) {
  auto performance_data = OperatorPerformanceData{};
  {
    std::stringstream stream;
    stream << performance_data;
    EXPECT_EQ(stream.str(), "not executed");
  }
  {
    std::stringstream stream;
    performance_data.executed = true;
    stream << performance_data;
    EXPECT_EQ(stream.str(), "executed, but no output");
  }
  {
    std::stringstream stream;
    performance_data.has_output = true;
    performance_data.output_row_count = 2u;
    performance_data.output_chunk_count = 1u;
    performance_data.walltime = std::chrono::nanoseconds{999u};
    stream << performance_data;
    EXPECT_EQ(stream.str(), "2 row(s) in 1 chunk(s), 999 ns");
  }
}

}  // namespace opossum
