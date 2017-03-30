#include "import_csv.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "csv.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

ImportCsv::ImportCsv(const std::string& directory, const std::string& filename, const std::string& tablename)
    : _directory(directory),
      _filename(filename),
      _tablename(tablename.empty() ? nullopt : optional<std::string>(tablename)) {}

const std::string ImportCsv::name() const { return "ImportCSV"; }

uint8_t ImportCsv::num_in_tables() const { return 0; }

uint8_t ImportCsv::num_out_tables() const { return 1; }

std::shared_ptr<const Table> ImportCsv::on_execute() {
  if (_tablename) {
    if (StorageManager::get().has_table(_tablename.value_or(""))) {
      return StorageManager::get().get_table(_tablename.value_or(""));
    }
  }

  auto table = _process_meta_file(_directory + '/' + _filename + csv::meta_file_extension);

  std::ifstream file;
  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  file.open(_directory + '/' + _filename + csv::file_extension);
  // allow failbit from std::getline
  file.exceptions(std::ifstream::badbit);

  std::string row;
  while (_get_row(file, row)) {
    const auto fields = _get_fields(row);
    if (table->col_count() != fields.size()) {
      throw std::runtime_error("Unexpected number of columns");
    }

    std::vector<AllTypeVariant> values(fields.size());

    for (ColumnID i = 0; i < table->col_count(); ++i) {
      values[i] = _convert(fields[i], table->column_type(i));
    }
    table->append(values);
  }

  if (_tablename) {
    StorageManager::get().add_table(_tablename.value_or(""), table);
  }

  return table;
}

const std::shared_ptr<Table> ImportCsv::_process_meta_file(const std::string& meta_file) {
  std::ifstream file;
  file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  file.open(meta_file);
  // allow failbit from std::getline
  file.exceptions(std::ifstream::badbit);

  std::string row;
  // skip header
  _get_row(file, row);

  // read chunk size
  _get_row(file, row);
  auto fields = _get_fields(row);
  const size_t chunk_size{std::stoul(fields[2])};
  const auto table = std::make_shared<Table>(chunk_size);

  // read column info
  while (_get_row(file, row)) {
    fields = _get_fields(row);
    table->add_column(fields[1], fields[2]);
  }
  return table;
}

AllTypeVariant ImportCsv::_convert(const std::string& value, const std::string& type) {
  if (type == "int") {
    return AllTypeVariant{std::stoi(value)};
  } else if (type == "long") {
    return AllTypeVariant{static_cast<int64_t>(std::stol(value))};
  } else if (type == "float") {
    return AllTypeVariant{std::stof(value)};
  } else if (type == "double") {
    return AllTypeVariant{std::stod(value)};
  } else if (type == "string") {
    return AllTypeVariant{value};
  }
  throw std::runtime_error("Invalid type");
}

std::string ImportCsv::_unescape(const std::string& input) {
  std::string result(input);

  // Remove quotes that surround the csv field
  if (input[0] == csv::quote && input.back() == csv::quote) {
    result.erase(0, 1);
    result.erase(result.size() - 1, 1);
  }

  // Remove escaping from escaped quotes.
  // Search for the string with csv::escape as first character and csv::quote as second character.
  const std::string search_pattern({csv::escape, csv::quote});
  for (auto pos = result.find(search_pattern, 0); pos != std::string::npos;
       pos = result.find(search_pattern, pos + 1)) {
    result.erase(pos, 1);
  }

  return result;
}

bool ImportCsv::_read_csv(std::istream& stream, std::string& out, const char delimiter) {
  out.clear();
  std::string line;

  while (std::getline(stream, line, delimiter)) {
    out += line;
    // If the number of quotes is even, "out" contains a full row.
    // If the number is odd, there is an opening quote but no closing quote. The delimiter is part of the field and must
    // appended in this case.
    // Escaped quotes are two quotes and therefore don't change the result of the modulus operation.
    if (std::count(out.begin(), out.end(), csv::quote) % 2 == 0) {
      return true;
    } else {
      out += delimiter;
    }
  }
  return false;
}

bool ImportCsv::_get_row(std::istream& stream, std::string& out) { return _read_csv(stream, out, csv::delimiter); }

bool ImportCsv::_get_field(std::istream& stream, std::string& out) { return _read_csv(stream, out, csv::separator); }

std::vector<std::string> ImportCsv::_get_fields(const std::string& row) {
  std::vector<std::string> fields;
  std::stringstream stream{row};
  std::string field;
  while (_get_field(stream, field)) {
    fields.emplace_back(_unescape(field));
  }
  return fields;
}

}  // namespace opossum
