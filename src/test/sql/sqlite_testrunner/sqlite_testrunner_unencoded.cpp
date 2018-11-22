#include "sqlite_testrunner.hpp"

namespace {

using namespace opossum;  // NOLINT

std::vector<SQLiteTestRunnerTestCase> generate_test_cases() {
  auto queries = SQLiteTestRunner::queries();

  std::vector<SQLiteTestRunnerTestCase> test_cases{queries.size()};

  for (auto test_case_idx = size_t{0}; test_case_idx < queries.size(); ++test_case_idx) {
    test_cases[test_case_idx] = SQLiteTestRunnerTestCase{queries[test_case_idx], false, EncodingType::Unencoded};
  }

  return test_cases;
}

}  // namespace

namespace opossum {

INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerUnencoded, SQLiteTestRunner,
                        testing::ValuesIn(generate_test_cases()), );  // NOLINT

}  // namespace opossum
