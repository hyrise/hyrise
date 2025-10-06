#include <gtest/gtest-param-test.h>

#include "sqlite_testrunner.hpp"
#include "storage/encoding_type.hpp"

namespace hyrise {

INSTANTIATE_TEST_SUITE_P(SQLiteTestRunnerUnencoded, SQLiteTestRunner,
                         testing::Combine(testing::ValuesIn(SQLiteTestRunner::queries()),
                                          testing::ValuesIn({EncodingType::Unencoded})),
                         sqlite_testrunner_formatter);

}  // namespace hyrise
