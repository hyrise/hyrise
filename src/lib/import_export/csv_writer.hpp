#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "csv_meta.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class CsvWriter {
 public:
  /*
   * Creates a new CsvWriter with the given file as output file.
   * @param file The file to output the csv to.
   */
  explicit CsvWriter(const std::string& file, const ParseConfig& config = {});

  template <typename T>
  void write(const T& value) {
    if (_current_col_count > 0) {
      _stream << _config.separator;
    }
    _write_value(value);
    ++_current_col_count;
  }

  /*
   * Ends a row of entries in the csv file.
   */
  void end_line();

 protected:
  std::string escape(const std::string& string);

  template <typename T>
  void _write_value(const T& value);

  std::ofstream _stream;
  ColumnID _current_col_count{0};
  ParseConfig _config;
};

template <typename T>
void CsvWriter::_write_value(const T& value) {
  _stream << value;
}

template <>
inline void CsvWriter::_write_value<std::string>(const std::string& value) {
  /* We put an the quotechars around any string value by default
   * as this is the only time when a comma (,) might be inside a value.
   * This might consume more space, however it speeds the program as it
   * does not require additional checks.
   * If we start allowing more characters as delimiter, we should change
   * this behaviour to either general quoting or checking for "illegal"
   * characters.
   */
  _stream << _config.quote;
  _stream << escape(value);
  _stream << _config.quote;
}

template <>
inline void CsvWriter::_write_value<AllTypeVariant>(const AllTypeVariant& value) {
  if (value.type() == typeid(std::string)) {
    _write_value(type_cast<std::string>(value));
  } else {
    _stream << value;
  }
}

}  // namespace opossum
