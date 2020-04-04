#include "csv_writer.hpp"

#include <map>
#include <string>
#include <utility>

#include <fstream>

#include <boost/algorithm/string.hpp>

namespace opossum {

CSVWriter::CSVWriter(
    const std::string file_path,
    const std::vector<std::string>
        headers,  // headers for csv file, if the file for file_path already exists, it must have equal headers
    bool override_file)
    : _headers(headers), _file_path(std::move(file_path)) {
  // Get path from string
  std::filesystem::path path_to_csv(_file_path);

  // Extract directory path from file
  const auto directory_path = path_to_csv.parent_path();

  // Check if directory already exists, if not create the directory
  if (!std::filesystem::exists(directory_path)) {
    std::filesystem::create_directories(directory_path);
  }

  // Check if directory_path is actually a directory.
  // If we do not create the directory, we cannot be sure if the provided path is valid
  DebugAssert(std::filesystem::is_directory(directory_path), directory_path.string() + " is not a directory.");

  // If we want to override the existing file, create a new one and insert headers.
  if (override_file) {
    _create_file_with_headers();
  }

  // Check existing file header is equal to new header in memory (important if replace_file = false)
  DebugAssert(([&]() -> bool {
                std::ifstream check_file;
                check_file.open(_file_path);
                std::string first_line;

                auto correct_header = false;

                getline(check_file, first_line);
                {
                  std::vector<std::string> to_test_headers;
                  boost::split(to_test_headers, first_line, [&](char c) { return c == _delimiter; });

                  if (to_test_headers == _headers) {
                    correct_header = true;
                  }
                }
                check_file.close();
                return correct_header;
              })(),
              "\nCSV Writer ERROR: Provided headers for file '" + _file_path +
                  "' does not match with existing headers. \n" + "Please validate headers for this CSVFile.");
}

void CSVWriter::write_row() {
  // Check if we have values for all columns
  DebugAssert(([&](std::map<std::string, std::string> map, std::vector<std::string> headers) -> bool {
                for (auto const& header : headers) {
                  // If header is not in current_row trigger error
                  if (_current_row.find(header) == _current_row.end()) {
                    return false;
                  }
                }
                return true;
              })(_current_row, _headers),
              "\nCSV Writer ERROR: Tried to write row to file '" + _file_path +
                  "' with missing values for one or more columns. \n" +
                  "Please validate header and value insertion. You might forgot a column or ");

  // construct row as string
  std::stringstream ss;
  auto header_size = _headers.size();
  for (uint64_t header_id = 0; header_id < header_size - 1; ++header_id) {  // header_size - 1 to skip last header
    auto header_label = _headers[header_id];
    ss << _current_row[header_label] << _delimiter;
  }
  ss << _current_row[_headers.back()] << std::endl;

  // save constructed row in file
  std::fstream file;
  file.open(_file_path, std::ofstream::out | std::ofstream::app);
  file << ss.str();
  file.close();

  // reset current row
  _current_row.clear();
}

void CSVWriter::_create_file_with_headers() const {
  // Save constructed row in file
  std::fstream file;
  file.open(_file_path, std::ofstream::out);

  std::stringstream ss;
  auto header_size = _headers.size();

  for (uint64_t header_id = 0; header_id < header_size - 1; ++header_id) {
    auto header_label = _headers[header_id];
    ss << header_label << _delimiter;
  }
  ss << _headers.back() << std::endl;

  file << ss.str();
  file.close();
}
}  // namespace opossum
