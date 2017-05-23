#include "csv_writer.hpp"

#include <fstream>
#include <string>
#include <vector>

#include "csv.hpp"
#include "types.hpp"

namespace opossum {

CsvWriter::CsvWriter(const std::string& file) {
  _stream.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  _stream.open(file);
}

/*
 * Escapes each quote character with an escape symbol.
 */
std::string CsvWriter::escape(const std::string& string) {
  std::string result(string);
  size_t next_pos = 0;
  while (std::string::npos != (next_pos = result.find(csv_quote, next_pos))) {
    result.insert(next_pos, 1, csv_escape);
    // Has to jump 2 positions ahead because a new character had been inserted.
    next_pos += 2;
  }
  return result;
}

void CsvWriter::write_line(const std::vector<AllTypeVariant>& values) {
  for (const auto& value : values) {
    write(value);
  }
  end_line();
}

void CsvWriter::end_line() {
  _stream << csv_delimiter;
  _current_col_count = 0;
}

}  // namespace opossum
