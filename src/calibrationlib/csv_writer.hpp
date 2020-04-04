#pragma once

#include <string>
#include <vector>

#include "boost/bimap.hpp"
#include "utils/assert.hpp"

namespace opossum {

class CSVWriter {
 public:
  CSVWriter(const std::string file_path, const std::vector<std::string> headers, const bool override_file = true);

  static constexpr std::string_view NA = "NULL";

  template <class T>
  void set_value(const std::string& key, const T& value) {
    std::stringstream ss;
    ss << value;
    const auto stringified_value = ss.str();

    // Check if we added a value in this row for a given column by accident
    DebugAssert(
        _current_row.find(key) == _current_row.end(),
        "\nCSV Writer ERROR: Added value for column '" + key + "' for file '" + _file_path + "' twice. \n" +
            "Please validate header and value insertion. Reminder: use write_row() to write and flush all values.");

    // Check if we added a value for an unknown column/header
    DebugAssert(std::find(_headers.begin(), _headers.end(), key) != _headers.end(),
                "\nCSV Writer ERROR: Added a value '" + stringified_value + "' for an unknown column '" + key +
                    "' for file '" + _file_path + "'\n" + "Please validate header and value insertion");

    // Check if delimiter is part of the added value which might lead to weird rows and import errors
    DebugAssert(stringified_value.find(_delimiter) == std::string::npos,
                "\nCSV Writer ERROR: Found delimiter '" + _delimiter + "' in value '" + stringified_value + "' of '" +
                    key + "' for file '" + _file_path + "'\n" +
                    "Please validate value insertion for this specific column or delimiter settings.");

    _current_row.insert(std::pair<std::string, std::string>(key, stringified_value));
  }

  void write_row();

 private:
  const std::vector<std::string> _headers;

  std::map<std::string, std::string> _current_row;

  const std::string _file_path;

  const char _delimiter = ',';
  void _create_file_with_headers() const;
};
}  //  namespace opossum
