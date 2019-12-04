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
  switch (_type) {
    case FileType::Auto:
      _write_any_type();
      break;
    case FileType::Csv:
      CsvWriter::write(*input_table_left(), _filename);
      break;
    case FileType::Binary:
      BinaryWriter::write(*input_table_left(), _filename);
      break;
    default:
      Fail("Exporting file type is not supported.");
  }

  return _input_left->get_output();
}

std::shared_ptr<AbstractOperator> Export::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Export>(copied_input_left, _filename, _type);
}

void Export::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void Export::_write_any_type() {
    std::vector<std::string> file_parts;
  boost::algorithm::split(file_parts, _filename, boost::is_any_of("."));
  const std::string& extension = file_parts.back();
  if (extension == "csv") {
    CsvWriter::write(*input_table_left(), _filename);
  } else if (extension == "bin") {
    BinaryWriter::write(*input_table_left(), _filename);
  } else {
    Fail("Cannot export file type.");
  }
}

}  // namespace opossum
