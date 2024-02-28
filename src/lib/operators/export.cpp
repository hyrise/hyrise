#include "export.hpp"

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <unordered_map>

#include <boost/algorithm/string.hpp>

#include "magic_enum.hpp"

#include "all_type_variant.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "import_export/file_type.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

Export::Export(const std::shared_ptr<const AbstractOperator>& input_operator, const std::string& filename,
               const FileType& file_type)
    : AbstractReadOnlyOperator(OperatorType::Export, input_operator), _filename(filename), _file_type(file_type) {
  if (_file_type == FileType::Auto) {
    _file_type = file_type_from_filename(filename);
  }
}

const std::string& Export::name() const {
  static const auto name = std::string{"Export"};
  return name;
}

std::string Export::description(DescriptionMode description_mode) const {
  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');

  auto file_type = std::string{magic_enum::enum_name(_file_type)};
  boost::algorithm::to_lower(file_type);
  return AbstractOperator::description(description_mode) + separator + "to '" + _filename + "'" + separator + "(" +
         file_type + ")";
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
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Export>(copied_left_input, _filename, _file_type);
}

void Export::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
