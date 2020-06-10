#include "export.hpp"

#include <boost/algorithm/string.hpp>

#include "hyrise.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "utils/assert.hpp"

namespace opossum {

Export::Export(const std::shared_ptr<const AbstractOperator>& in, const std::string& filename,
               const FileType& file_type)
    : AbstractReadOnlyOperator(OperatorType::Export, in), _filename(filename), _file_type(file_type) {
  if (_file_type == FileType::Auto) {
    _file_type = file_type_from_filename(filename);
  }
}

const std::string& Export::name() const {
  static const auto name = std::string{"Export"};
  return name;
}

std::shared_ptr<const Table> Export::_on_execute() {
  if (_filename.empty() || std::all_of(_filename.begin(), _filename.end(), isspace)) {
    Fail("Export: File name must not be empty.");
  }

  switch (_file_type) {
    case FileType::Csv:
      CsvWriter::write(*left_input_table(), _filename);
      break;
    case FileType::Binary:
      BinaryWriter::write(*left_input_table(), _filename);
      break;
    case FileType::Auto:
    case FileType::Tbl:
      Fail("Export: Exporting file type is not supported.");
  }

  // must match ExportNode::output_expressions
  return nullptr;
}

std::shared_ptr<AbstractOperator> Export::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<Export>(copied_left_input, _filename, _file_type);
}

void Export::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
