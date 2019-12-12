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
    : public BaseTestWithParam<std::tuple<DataType, EncodingType, std::optional<OrderByMode>, bool /*nullable*/>> {
 public:
  static std::string format(testing::TestParamInfo<ParamType> info) {
    const auto& [data_type, encoding, order_mode, nullable] = info.param;

    return data_type_to_string.left.at(data_type) + encoding_type_to_string.left.at(encoding) +
           (order_mode ? order_by_mode_to_string.left.at(*order_mode) : "Unordered") + (nullable ? "" : "Not") +
           "Nullable";
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
      for (auto ordered_by_it = order_by_mode_to_string.begin(); ordered_by_it != order_by_mode_to_string.end();
           ++ordered_by_it) {
        const auto& ordered_by_mode = ordered_by_it->left;
        pairs.emplace_back(data_type, encoding, ordered_by_mode, true);
        pairs.emplace_back(data_type, encoding, ordered_by_mode, false);
      }
      pairs.emplace_back(data_type, encoding, std::nullopt, true);
      pairs.emplace_back(data_type, encoding, std::nullopt, false);
    }
  });
  return pairs;
}
}  // namespace opossum
