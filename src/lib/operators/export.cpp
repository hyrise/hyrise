#include "export.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <numeric>
#include <utility>

#include <boost/algorithm/string.hpp>

#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

Export::Export(const std::shared_ptr<const AbstractOperator>& in, const std::string& file_name)
    : AbstractReadOnlyOperator(OperatorType::Export, in), _file_name(file_name) {}
const std::string& Import::name() const {
  static const auto name = std::string{"Export"};
  return name;
}

std::shared_ptr<const Table> Export::_on_execute() {
  return nullptr:
}

std::shared_ptr<AbstractOperator> Export::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Export>(copied_input_left, _file_name);
}

void Import::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
