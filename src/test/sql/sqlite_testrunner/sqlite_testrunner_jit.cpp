#include "sqlite_testrunner.hpp"

namespace opossum {

INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerJIT, SQLiteTestRunner,
                        testing::Combine(testing::ValuesIn(SQLiteTestRunner::queries()), testing::ValuesIn({true}),
                                         testing::ValuesIn({EncodingType::Unencoded})), );  // NOLINT

}  // namespace opossum
