#pragma once

#include <cmath>
#include <memory>
#include <optional>

#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace opossum {

class Table;

enum class DataDistributionType { Uniform, NormalSkewed, Pareto };

struct ColumnDataDistribution {
  static ColumnDataDistribution make_uniform_config(const double min, const double max) {
    ColumnDataDistribution c{};
    c.min_value = min;
    c.max_value = max;
    c.num_different_values = static_cast<int>(std::floor(max - min));
    return c;
  }

  static ColumnDataDistribution make_pareto_config(const double pareto_scale = 1.0, const double pareto_shape = 1.0) {
    ColumnDataDistribution c{};
    c.pareto_scale = pareto_scale;
    c.pareto_shape = pareto_shape;
    c.distribution_type = DataDistributionType::Pareto;
    return c;
  }

  static ColumnDataDistribution make_skewed_normal_config(
      const double skew_location = 0.0, const double skew_scale = 1.0,
      // Temporary work around for https://github.com/boostorg/math/issues/254.
      // TODO(anyone): reset to 0.0 when Hyrise's boost has the fix.
      const double skew_shape = 0.0001) {
    ColumnDataDistribution c{};
    c.skew_location = skew_location;
    c.skew_scale = skew_scale;
    c.skew_shape = skew_shape;
    c.distribution_type = DataDistributionType::NormalSkewed;
    return c;
  }

  DataDistributionType distribution_type = DataDistributionType::Uniform;

  int num_different_values = 1'000;

  double pareto_scale;
  double pareto_shape;

  double skew_location;
  double skew_scale;
  double skew_shape;

  double min_value;
  double max_value;
};

class SyntheticTableGenerator {
 public:
  // Simple table generation, mainly for simple tests
  std::shared_ptr<Table> generate_table(const size_t num_columns, const size_t num_rows, const ChunkOffset chunk_size,
                                        const SegmentEncodingSpec segment_encoding_spec = {EncodingType::Unencoded});

  static std::shared_ptr<Table> generate_table(
      const std::vector<ColumnDataDistribution>& column_data_distributions,
      const std::vector<DataType>& column_data_types, const size_t num_rows, const ChunkOffset chunk_size,
      const std::optional<ChunkEncodingSpec>& segment_encoding_specs = std::nullopt,
      const std::optional<std::vector<std::string>>& column_names = std::nullopt, const UseMvcc use_mvcc = UseMvcc::No);

  /**
    * Function to create a typed value from an integer. The data generation creates integers with the requested
    * distribution and this function is used to create different types. The creation should guarantee that searching
    * for the integer value 100 within a column of the range (0,200) should return half of all tuples, not only for
    * ints but also for all other value types such as strings.
    * Handling of values:
    *   - in case of long, the integer is simply casted
    *   - in case of floating types, the integer is slightly modified and casted to ensure a mantissa that is not fully
    *     zero'd. We have stumbled about operators using radix clustering (e.g., some joins) which did not show certain
    *     problems when all floating values had a mostly zero'd mantissa. The modification adds noise to make the
    *     floating point values in some sense more realistic.a
    *   - in case of strings, a 10 char string is created that has at least `prefix_length` leading spaces to ensure
    *     that scans have to evaluate at least the first four chars. The reason is that very often strings are dates in
    *     in ERP systems and we assume that at least the year has to be read before a non-match can be determined.
    *     Randomized strings often lead to unrealistically fast string scans. An example would be randomized strings in
    *     which a typical linear scan only has to read the first char in order to disqualify a tuple. Strings in real
    *     world systems often share a common prefix (e.g., country code prefixes or dates starting with the year) where
    *     usually more chars need to be read. The observed effect was that operations on randomized strings were
    *     unexpectedly faster than seen with real-world data.
    *     Example (shortening leading spaces): (0, 1, 2, 10, 11, 61, 62, 75) >>
    *                                          ('   ', '  1', '  2', '  A', '  B', '  z', ' 10', ' 11', ' 1D')
    */
  template <typename T>
  static T generate_value(const int input) {
    if constexpr (std::is_integral_v<T>) {
      return static_cast<T>(input);
    } else if constexpr (std::is_floating_point_v<T>) {
      return static_cast<T>(input) * 0.999999f;
    } else if constexpr (std::is_same_v<T, pmr_string>) {
      Assert(input >= 0, "Integer values need to be positive in order to be converted to a pmr_string.");

      constexpr auto generated_string_length = size_t{10};
      constexpr auto prefix_length = size_t{4};
      constexpr auto variable_string_length = generated_string_length - prefix_length;

      const std::vector<char> chars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
                                       'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
                                       'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                                       'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
      const size_t chars_base = chars.size();
      Assert(static_cast<double>(input) < std::pow(chars_base, variable_string_length),
             "Input too large. Cannot be represented in " + std::to_string(variable_string_length) + " chars.");

      pmr_string result(generated_string_length, ' ');  // fill full length with spaces
      if (input == 0) {
        return result;
      }

      const size_t result_char_count = static_cast<size_t>(std::floor(std::log(input) / std::log(chars_base)) + 1);
      size_t remainder = static_cast<size_t>(input);
      for (auto i = size_t{0}; i < result_char_count; ++i) {
        result[generated_string_length - 1 - i] = chars[remainder % chars_base];
        remainder = static_cast<size_t>(remainder / chars_base);
      }

      return result;
    }
  }

 protected:
  const int _max_different_value = 10'000;
};

}  // namespace opossum
