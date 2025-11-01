#include <vector>

#include "gtest/gtest.h"
#include "sqlite_testrunner.hpp"
#include "storage/encoding_type.hpp"

namespace hyrise {

namespace {

std::vector<EncodingType> encoding_types_without_unencoded() {
  static auto encodings = std::vector<EncodingType>{};

  if (encodings.empty()) {
    for (const auto encoding_type : ENCODING_TYPES) {
      if (encoding_type != EncodingType::Unencoded) {
        encodings.push_back(encoding_type);
      }
    }
  }

  return encodings;
}

}  // namespace

INSTANTIATE_TEST_SUITE_P(SQLiteTestRunnerEncodings, SQLiteTestRunner,
                         testing::Combine(::testing::ValuesIn(SQLiteTestRunner::queries()),
                                          ::testing::ValuesIn(encoding_types_without_unencoded())),
                         sqlite_testrunner_formatter);

}  // namespace hyrise
