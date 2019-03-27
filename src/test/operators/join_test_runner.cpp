#include "base_test.hpp"

#include <iostream>

#include "json.hpp"

namespace {

struct JoinTestRunnerParameter {

};

}  // namespace

namespace opossum {

class JoinTestRunner : BaseTestWithParam<JoinTestRunnerParameter> {
 public:
  static std::vector<JoinTestRunnerParameter> load_parameters() {
    auto parameter_file = std::ifstream{"resources"};
  }


};

INSTANTIATE_TEST_CASE_P(
JoinTestRunnerInstance, JoinTestRunner, testing::ValuesIn(JoinTestRunner::load_parameters()), );  // NOLINT(whitespace/parens)  // NOLINT

}  // namespace opossum
