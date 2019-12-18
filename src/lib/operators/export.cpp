#include "export.hpp"

#include <boost/algorithm/string.hpp>

#include "hyrise.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "utils/assert.hpp"

namespace opossum {

Export::Export(const std::shared_ptr<const AbstractOperator>& in, const std::string& filename, const FileType& type)
    : AbstractReadOnlyOperator(OperatorType::Export, in), _filename(filename), _type(type) {}

const std::string& Export::name() const {
  static const auto name = std::string{"Export"};
  return name;
}

std::shared_ptr<const Table> Export::_on_execute() {
  if (_filename.empty() || std::all_of(_filename.begin(), _filename.end(), isspace)) {
    Fail("Export: File name must not be empty.");
  }

  if (_type == FileType::Auto) {
    _type = file_type_from_filename(_filename);
  }

  switch (_type) {
    case FileType::Csv:
      CsvWriter::write(*input_table_left(), _filename);
      break;
    case FileType::Binary:
      BinaryWriter::write(*input_table_left(), _filename);
      break;
    case FileType::Auto:
    case FileType::Tbl:
      Fail("Export: Exporting file type is not supported.");
  }

  return _input_left->get_output();
}

std::shared_ptr<AbstractOperator> Export::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Export>(copied_input_left, _filename, _type);
}

void Export::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
