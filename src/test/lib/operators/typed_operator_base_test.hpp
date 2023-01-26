#pragma once

#include <optional>

#include <boost/hana/for_each.hpp>

#include "base_test.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace hyrise {
// This is the base class for all tests that should run on every possible combination of data type, order and encoding.
// A sample table is generate in TableScanBetweenTest. Depending on how other tests will use this, it might make sense
// to have a single instantiation here
class TypedOperatorBaseTest
    : public BaseTestWithParam<std::tuple<DataType, EncodingType, std::optional<SortMode>, bool /*nullable*/>> {
 public:
  static std::string format(testing::TestParamInfo<ParamType> info) {
    const auto& [data_type, encoding, sort_mode, nullable] = info.param;
    auto output = std::stringstream{};

    output << data_type << encoding << (sort_mode ? magic_enum::enum_name(*sort_mode) : "Unsorted")
           << (nullable ? "" : "Not") << "Nullable";

    return output.str();
  }
};

static std::vector<TypedOperatorBaseTest::ParamType> create_test_params() {
  std::vector<TypedOperatorBaseTest::ParamType> pairs;

  hana::for_each(data_type_pairs, [&](auto pair) {
    const auto& data_type = hana::first(pair);

    for (const auto encoding : magic_enum::enum_values<EncodingType>()) {
      if (!encoding_supports_data_type(encoding, data_type)) {
        continue;
      }

      for (const auto sort_mode : magic_enum::enum_values<SortMode>()) {
        pairs.emplace_back(data_type, encoding, sort_mode, true);
        pairs.emplace_back(data_type, encoding, sort_mode, false);
      }
      pairs.emplace_back(data_type, encoding, std::nullopt, true);
      pairs.emplace_back(data_type, encoding, std::nullopt, false);
    }
  });
  return pairs;
}
}  // namespace hyrise
