#include "sqlite_testrunner.hpp"

namespace opossum {

INSTANTIATE_TEST_SUITE_P(SQLiteTestRunnerUnencoded, SQLiteTestRunner,
                         testing::Combine(testing::ValuesIn(SQLiteTestRunner::queries()),
                                          testing::ValuesIn({EncodingType::Unencoded})),
                         sqlite_testrunner_formatter);

}  // namespace opossum
