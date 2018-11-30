#include "sqlite_testrunner.hpp"

namespace opossum {

#if HYRISE_JIT_SUPPORT

INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerJIT, SQLiteTestRunner,
                        testing::Combine(testing::ValuesIn(SQLiteTestRunner::queries()), testing::ValuesIn({true}),
                                         testing::ValuesIn({EncodingType::Unencoded})), );  // NOLINT

#endif

}  // namespace opossum
