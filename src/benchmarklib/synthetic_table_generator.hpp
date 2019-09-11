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
  static ColumnDataDistribution make_uniform_config(double min, double max) {
    ColumnDataDistribution c{};
    c.min_value = min;
    c.max_value = max;
    c.num_different_values = static_cast<int>(std::floor(max - min));
    return c;
  }

  static ColumnDataDistribution make_pareto_config(double pareto_scale = 1.0, double pareto_shape = 1.0) {
    ColumnDataDistribution c{};
    c.pareto_scale = pareto_scale;
    c.pareto_shape = pareto_shape;
    c.distribution_type = DataDistributionType::Pareto;
    return c;
  }

  static ColumnDataDistribution make_skewed_normal_config(double skew_location = 0.0, double skew_scale = 1.0,
                                                          double skew_shape = 0.0) {
    ColumnDataDistribution c{};
    c.skew_location = skew_location;
    c.skew_scale = skew_scale;
    c.skew_shape = skew_shape;
    c.distribution_type = DataDistributionType::NormalSkewed;
    return c;
  }

  DataDistributionType distribution_type = DataDistributionType::Uniform;

  int num_different_values = 1'000;

  double pareto_scale = 1.0;
  double pareto_shape = 1.0;

  double skew_location = 0.0;
  double skew_scale = 1.0;
  double skew_shape = 0.0;

  double min_value = 0.0;
  double max_value = 1.0;
};

class SyntheticTableGenerator {
  // Note: numa_distribute_chunks=true only affects generated tables that use DictionaryCompression,
  // otherwise the chunks are most likely all placed on a single node. This might change in the future.
  // See the discussion here https://github.com/hyrise/hyrise/pull/402
 public:
  // Simple table generation, mainly for simple tests
  std::shared_ptr<Table> generate_table(const size_t num_columns, const size_t num_rows, const ChunkOffset chunk_size,
                                        const SegmentEncodingSpec segment_encoding_spec = {EncodingType::Unencoded});

  std::shared_ptr<Table> generate_table(const std::vector<ColumnDataDistribution>& column_data_distributions,
                                        const std::vector<DataType>& column_data_types, const size_t num_rows,
                                        const ChunkOffset chunk_size,
                                        const std::vector<SegmentEncodingSpec>& segment_encoding_specs,
                                        const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                                        const UseMvcc use_mvcc = UseMvcc::No,
                                        const bool numa_distribute_chunks = false);

  // Base function that generates the actual data
  std::shared_ptr<Table> generate_table(const std::vector<ColumnDataDistribution>& column_data_distributions,
                                        const std::vector<DataType>& column_data_types, const size_t num_rows,
                                        const ChunkOffset chunk_size,
                                        const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                                        const UseMvcc use_mvcc = UseMvcc::No,
                                        const bool numa_distribute_chunks = false);

  /**
    * Function to cast an integer to the requested type.
    *   - in case of long, the integer is simply casted
    *   - in case of floating types, the integer slighlty modified
    *     and casted to ensure a matissa that is not fully zero'd
    *   - in case of strings, a 10 char string is created that has at least
    *     `prefix_length` leading spaces to ensure that scans have to evaluate
    *     at least the first four chars. The reason is that very often strings
    *     are dates in ERP systems and we assume that at least the year has to
    *     be read before a non-match can be determined. Randomized strings
    *     often lead to unrealistically fast string scans.
    */
  template <typename T>
  static T convert_integer_value(const int input) {
    if constexpr (std::is_integral_v<T>) {
      return static_cast<T>(input);
    } else if constexpr (std::is_floating_point_v<T>) {
      // floating points are slightly shifted to avoid a zero'd mantissa.
      return static_cast<T>(input) * 0.999999f;
    } else {
      constexpr auto generated_string_length = size_t{10};
      constexpr auto prefix_length = size_t{4};
      constexpr auto variable_string_length = generated_string_length - prefix_length;

      Assert(input >= 0, "Integer values need to be positive in order to be converted to a pmr_string.");

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
  const size_t _num_columns = 10;
  const size_t _num_rows = 40'000;
  const int _max_different_value = 10'000;
};

}  // namespace opossum
