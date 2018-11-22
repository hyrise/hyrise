#include "sqlite_testrunner.hpp"

#include "boost/hana.hpp"

namespace {

using namespace opossum;  // NOLINT

std::vector<SQLiteTestRunnerTestCase> generate_test_cases() {
  auto queries = SQLiteTestRunner::queries();

  const auto encoding_type_count = hana::size(supported_data_types_for_encoding_type);

  std::vector<SQLiteTestRunnerTestCase> test_cases;
  test_cases.reserve(queries.size() * encoding_type_count);

  hana::for_each(supported_data_types_for_encoding_type, [&](auto encoding_type_and_data_types) {
    const auto encoding_type = std::decay_t<decltype(hana::first(encoding_type_and_data_types))>::value;

    // "No encoding" is already tested by the SQLiteTestRunnerUnencoded
    if (encoding_type == EncodingType::Unencoded) return;

    for (auto test_case_idx = size_t{0}; test_case_idx < queries.size(); ++test_case_idx) {
      test_cases.emplace_back(SQLiteTestRunnerTestCase{queries[test_case_idx], false, encoding_type});
    }
  });

  return test_cases;
}

}  // namespace

namespace opossum {

INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerEncodings, SQLiteTestRunner,
                        testing::ValuesIn(generate_test_cases()), );  // NOLINT

}  // namespace opossum
