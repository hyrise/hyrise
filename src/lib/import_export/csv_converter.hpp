#pragma once

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "csv.hpp"
#include "storage/base_column.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

/*
 * CsvConverter is a helper class that creates a ValueColumn by converting the given null terminated strings and placing
 * them at the given position.
 * The base class AbstractCsvConverter allows us to handle different types of ColumnCreators uniformly.
 */

class AbstractCsvConverter {
 public:
  virtual ~AbstractCsvConverter() = default;

  // Converts value to the underlying data type and saves it at the given position.
  virtual void insert(const std::string &value, ChunkOffset position) = 0;

  // Returns the Column which contains the previously converted values.
  // After the call of finish, no other operation should be called.
  virtual std::unique_ptr<BaseColumn> finish() = 0;

  /*
   * This is a helper function that removes surrounding quotes of the given csv field and all escape characters.
   * The operation is in-place and does not create a new string object.
   * Field must be a valid csv field.
   */
  static void unescape(std::string &field, const CsvConfig &config = {}) {
    // String does not contain escaping if it is not surrounded with quotes
    if (field.empty() || field.front() != config.quote) return;

    std::string unescaped_string;
    unescaped_string.reserve(field.size());

    // last_char holds the value that the last lambda call visited. It can have any start value except for config.escape
    char last_char = 0;
    // The start and end ranges leave out the surrounding quotes.
    // Since config.escape and config.quote are the same characters, we can remove the quote instead of the escape
    // character.
    std::copy_if(field.begin() + 1, field.end() - 1, std::back_inserter(unescaped_string),
                 [&last_char, &config](const char c) {
                   bool do_copy = last_char != config.escape || c != config.quote;
                   // Set last_char to zero if the current character should not be copied
                   // This is necessary because config.escape and config.quote are the same characters
                   // and therefore a sequence of three quotes would trigger the condition above twice.
                   last_char = do_copy ? c : 0;
                   return do_copy;
                 });

    unescaped_string.shrink_to_fit();
    field = std::move(unescaped_string);
  }
};

template <typename T>
class CsvConverter : public AbstractCsvConverter {
 public:
  explicit CsvConverter(ChunkOffset size, const CsvConfig &config = {}) : _parsed_values(size), _config(config) {}

  void insert(const std::string &value, ChunkOffset position) override {
    _parsed_values[position] = _get_conversion_function()(value);
  }

  std::unique_ptr<BaseColumn> finish() override { return std::make_unique<ValueColumn<T>>(std::move(_parsed_values)); }

 private:
  /*
   * Returns a conversion function that converts from a string to type T.
   * This function is defined for each type that can be stored in a ValueColumn.
   * The assumption is that only csv fields of type string must be unescaped because other types cannot contain special
   * csv characters.
   */
  std::function<T(const std::string &)> _get_conversion_function();
  tbb::concurrent_vector<T> _parsed_values;
  CsvConfig _config;
};

template <>
inline std::function<int(const std::string &)> CsvConverter<int>::_get_conversion_function() {
  return [](const std::string &str) { return std::stoi(str); };
}

template <>
inline std::function<int64_t(const std::string &)> CsvConverter<int64_t>::_get_conversion_function() {
  return [](const std::string &str) { return static_cast<int64_t>(std::stoll(str)); };
}

template <>
inline std::function<float(const std::string &)> CsvConverter<float>::_get_conversion_function() {
  return [](const std::string &str) { return std::stof(str); };
}

template <>
inline std::function<double(const std::string &)> CsvConverter<double>::_get_conversion_function() {
  return [](const std::string &str) { return std::stod(str); };
}

template <>
inline std::function<std::string(const std::string &)> CsvConverter<std::string>::_get_conversion_function() {
  return [this](const std::string &str) {
    std::string copy = str;
    unescape(copy, _config);
    return copy;
  };
}

}  // namespace opossum
