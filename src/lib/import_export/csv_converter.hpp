#pragma once

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <boost/algorithm/string.hpp>

#include "csv_meta.hpp"
#include "storage/base_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/*
 * CsvConverter is a helper class that creates a ValueSegment by converting the given null terminated strings and placing
 * them at the given position.
 * The base class BaseCsvConverter allows us to handle different types of columns uniformly.
 */

class BaseCsvConverter {
 public:
  virtual ~BaseCsvConverter() = default;

  // Converts value to the underlying data type and saves it at the given position.
  virtual void insert(std::string& value, ChunkOffset position) = 0;

  // Returns the segment that contains the previously converted values.
  // After the call of finish, no other operation should be called.
  virtual std::unique_ptr<BaseSegment> finish() = 0;

  /*
   * This is a helper function that removes surrounding quotes of the given csv field and all escape characters.
   * The operation is in-place and does not create a new string object.
   * Field must be a valid csv field.
   */
  static void unescape(std::string& field, const ParseConfig& config = {});
  static std::string unescape_copy(const std::string& field, const ParseConfig& config = {});
};

template <typename T>
class CsvConverter : public BaseCsvConverter {
 public:
  explicit CsvConverter(ChunkOffset size, const ParseConfig& config = {}, bool is_nullable = false)
      : _parsed_values(size), _null_values(size, false), _is_nullable(is_nullable), _config(config) {}

  void insert(std::string& value, ChunkOffset position) override {
    if (_is_nullable && value.length() == 0) {
      _null_values[position] = true;
      return;
    }

    if (boost::to_lower_copy(value) == ParseConfig::NULL_STRING) {
      Assert(_config.null_handling != NullHandling::RejectNullStrings,
             "Unquoted null found in CSV file. Quote it for string literal \"null\", leave field empty for null "
             "value, or set 'null_handling' to the appropriate strategy in parse config.");
      if (_is_nullable && _config.null_handling == NullHandling::NullStringAsNull) {
        _null_values[position] = true;
        return;
      }
    }

    // clang-format off
    if constexpr(std::is_same_v<T, pmr_string>) {
      unescape(value, _config);
    } else {  // NOLINT
      // clang-format on
      if (_config.reject_quoted_nonstrings) {
        Assert(value == unescape_copy(value, _config),
               "Unexpected quoted string " + value + " encountered in non-string column");
      } else {
        unescape(value, _config);
      }
    }

    _parsed_values[position] = _get_conversion_function()(value);
  }

  std::unique_ptr<BaseSegment> finish() override {
    if (_is_nullable) {
      return std::make_unique<ValueSegment<T>>(std::move(_parsed_values), std::move(_null_values));
    } else {
      return std::make_unique<ValueSegment<T>>(std::move(_parsed_values));
    }
  }

 private:
  /*
   * Returns a conversion function that converts from a string to type T.
   * This function is defined for each type that can be stored in a ValueSegment.
   * The assumption is that only csv fields of type string must be unescaped because other types cannot contain special
   * csv characters.
   */
  std::function<T(const std::string&)> _get_conversion_function();
  pmr_concurrent_vector<T> _parsed_values;
  pmr_concurrent_vector<bool> _null_values;
  const bool _is_nullable;
  ParseConfig _config;
};

template <>
inline std::function<int32_t(const std::string&)> CsvConverter<int32_t>::_get_conversion_function() {
  return [](const std::string& str) {
    size_t pos;
    auto converted = std::stoi(str, &pos);
    Assert(pos == str.size(), "Unprocessed characters found while converting to int: " + str);
    return converted;
  };
}

template <>
inline std::function<int64_t(const std::string&)> CsvConverter<int64_t>::_get_conversion_function() {
  return [](const std::string& str) {
    size_t pos;
    auto converted = static_cast<int64_t>(std::stoll(str, &pos));
    Assert(pos == str.size(), "Unprocessed characters found while converting to long: " + str);
    return converted;
  };
}

template <>
inline std::function<float(const std::string&)> CsvConverter<float>::_get_conversion_function() {
  return [](const std::string& str) {
    size_t pos;
    auto converted = std::stof(str, &pos);
    Assert(pos == str.size(), "Unprocessed characters found while converting to float: " + str);
    return converted;
  };
}

template <>
inline std::function<double(const std::string&)> CsvConverter<double>::_get_conversion_function() {
  return [](const std::string& str) {
    size_t pos;
    auto converted = std::stod(str, &pos);
    Assert(pos == str.size(), "Unprocessed characters found while converting to double: " + str);
    return converted;
  };
}

template <>
inline std::function<pmr_string(const std::string&)> CsvConverter<pmr_string>::_get_conversion_function() {
  return [](const std::string& str) { return pmr_string{str}; };
}

}  // namespace opossum
