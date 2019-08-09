#include "sqlite_testrunner.hpp"

#include "storage/encoding_type.hpp"

namespace opossum {

INSTANTIATE_TEST_CASE_P(SQLiteTestRunnerEncodings, SQLiteTestRunner,
                        testing::Combine(testing::ValuesIn(SQLiteTestRunner::queries()), testing::ValuesIn({false}),
                                         testing::ValuesIn({EncodingType::Dictionary, EncodingType::RunLength,
                                                            EncodingType::FixedStringDictionary,
                                                            EncodingType::FrameOfReference})), );  // NOLINT

}  // namespace opossum
