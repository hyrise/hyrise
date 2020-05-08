#pragma once

#include <optional>

#include <boost/hana/for_each.hpp>

#include "base_test.hpp"
#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace opossum {
// This is the base class for all tests that should run on every possible combination of data type, order and encoding.
// A sample table is generate in TableScanBetweenTest. Depending on how other tests will use this, it might make sense
// to have a single instantiation here
class TypedOperatorBaseTest
    : public BaseTestWithParam<std::tuple<DataType, EncodingType, std::optional<SortMode>, bool /*nullable*/>> {
 public:
  static std::string format(testing::TestParamInfo<ParamType> info) {
    const auto& [data_type, encoding, sort_mode, nullable] = info.param;

    return data_type_to_string.left.at(data_type) + encoding_type_to_string.left.at(encoding) +
           (sort_mode ? sort_mode_to_string.left.at(*sort_mode) : "Unsorted") + (nullable ? "" : "Not") + "Nullable";
  }
};

static std::vector<TypedOperatorBaseTest::ParamType> create_test_params() {
  std::vector<TypedOperatorBaseTest::ParamType> pairs;

  hana::for_each(data_type_pairs, [&](auto pair) {
    const auto& data_type = hana::first(pair);

    for (auto encoding_it = encoding_type_to_string.begin(); encoding_it != encoding_type_to_string.end();
         ++encoding_it) {
      const auto& encoding = encoding_it->left;
      if (!encoding_supports_data_type(encoding, data_type)) continue;
      for (auto sorted_by_it = sort_mode_to_string.begin(); sorted_by_it != sort_mode_to_string.end(); ++sorted_by_it) {
        const auto& sort_mode = sorted_by_it->left;
        pairs.emplace_back(data_type, encoding, sort_mode, true);
        pairs.emplace_back(data_type, encoding, sort_mode, false);
      }
      pairs.emplace_back(data_type, encoding, std::nullopt, true);
      pairs.emplace_back(data_type, encoding, std::nullopt, false);
    }
  });
  return pairs;
}
}  // namespace opossum
