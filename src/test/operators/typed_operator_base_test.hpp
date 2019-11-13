#pragma once

#include <boost/hana/for_each.hpp>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

// This is the base class for all tests that should run on every possible combination of data type and encoding.
// A sample table is generate in TableScanBetweenTest. Depending on how other tests will use this, it might make sense
// to have a single instantiation here.
class TypedOperatorBaseTest : public BaseTestWithParam<std::tuple<DataType, EncodingType, bool /* nullable */>> {
 public:
  static std::string format(testing::TestParamInfo<ParamType> info) {
    const auto& [data_type, encoding, nullable] = info.param;

    return data_type_to_string.left.at(data_type) + encoding_type_to_string.left.at(encoding) +
           (nullable ? "" : "Not") + "Nullable";
  }
};

static std::vector<TypedOperatorBaseTest::ParamType> create_test_params() {
  std::vector<TypedOperatorBaseTest::ParamType> pairs;

  hana::for_each(data_type_pairs, [&](auto pair) {
    const auto& data_type = hana::first(pair);

    for (auto it = encoding_type_to_string.begin(); it != encoding_type_to_string.end(); ++it) {
      const auto& encoding = it->left;
      if (!encoding_supports_data_type(encoding, data_type)) continue;
      pairs.emplace_back(data_type, encoding, true);
      pairs.emplace_back(data_type, encoding, false);
    }
  });

  return pairs;
}

}  // namespace opossum
