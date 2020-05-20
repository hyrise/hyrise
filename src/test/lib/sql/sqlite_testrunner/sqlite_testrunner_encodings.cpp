#include "sqlite_testrunner.hpp"

#include "storage/encoding_type.hpp"

namespace opossum {

INSTANTIATE_TEST_SUITE_P(SQLiteTestRunnerEncodings, SQLiteTestRunner,
                         testing::Combine(::testing::ValuesIn(SQLiteTestRunner::queries()),
                                          ::testing::ValuesIn(all_encoding_types)),
                         sqlite_testrunner_formatter);

}  // namespace opossum
