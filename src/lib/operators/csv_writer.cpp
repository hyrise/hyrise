#include "csv_writer.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "csv.hpp"
#include "types.hpp"

namespace opossum {

CsvWriter::CsvWriter(const std::string& file) : _stream(file) {}

void CsvWriter::write(const AllTypeVariant& value) {
  // Place a delimiter before the current value if there have been values written before
  if (_current_col_count > 0) {
    _stream << csv::separator;
  }
  /*
   * Check if value is of type string in order to handle escaping.
   */
  if (value.type() == typeid(std::string)) {
    /* We put an the quotechars around any string value by default
     * as this is the only time when a comma (,) might be inside a value.
     * This might consume more space, however it speeds the program as it
     * does not require additional checks.
     * If we start allowing more characters as delimiter, we should change
     * this behaviour to either general quoting or checking for "illegal"
     * characters.
     */
    std::string str = boost::lexical_cast<std::string>(value);
    escape(str);
    _stream << csv::quote;
    _stream << str;
    _stream << csv::quote;
  } else {
    _stream << value;
  }
  this->_current_col_count++;
}

/*
 * Escapes each quote character with an escape symbol.
 */
void CsvWriter::escape(std::string& string) {
  size_t next_pos = 0;
  while (std::string::npos != (next_pos = string.find(csv::quote, next_pos))) {
    string.insert(next_pos, 1, csv::escape);
    // Has to jump 2 positions ahead because a new character had been inserted.
    next_pos += 2;
  }
}

void CsvWriter::write_line(const std::vector<AllTypeVariant>& values) {
  for (const auto& value : values) {
    write(value);
  }
  end_line();
}

void CsvWriter::end_line() {
  _stream << csv::delimiter;
  _current_col_count = 0;
}

}  // namespace opossum
