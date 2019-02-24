#include "sqlite_testrunner.hpp"

namespace opossum {

#if HYRISE_JIT_SUPPORT

// Test JIT with unencoded and dictionary encoded tables (the latter just for good measure)
INSTANTIATE_TEST_CASE_P(
    SQLiteTestRunnerJIT, SQLiteTestRunner,
    testing::Combine(testing::ValuesIn(SQLiteTestRunner::queries()), testing::ValuesIn({true}),
                     testing::ValuesIn({EncodingType::Unencoded,
                                        EncodingType::Dictionary})), );  // NOLINT(whitespace/parens)  // NOLINT

#endif

}  // namespace opossum
