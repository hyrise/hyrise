#include "alias_operator.hpp"

#include <sstream>

#include "boost/algorithm/string/join.hpp"
#include "storage/table.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

AliasOperator::AliasOperator(const std::shared_ptr<const AbstractOperator>& input,
                             const std::vector<CxlumnID>& cxlumn_ids, const std::vector<std::string>& aliases)
    : AbstractReadOnlyOperator(OperatorType::Alias, input, nullptr), _cxlumn_ids(cxlumn_ids), _aliases(aliases) {
  Assert(_cxlumn_ids.size() == _aliases.size(), "Expected as many aliases as cxlumns");
}

const std::string AliasOperator::name() const { return "Alias"; }

const std::string AliasOperator::description(DescriptionMode description_mode) const {
  std::stringstream stream;
  stream << "Alias [";
  stream << boost::algorithm::join(_aliases, description_mode == DescriptionMode::SingleLine ? ", " : "\n");
  stream << "]";
  return stream.str();
}

std::shared_ptr<AbstractOperator> AliasOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<AliasOperator>(copied_input_left, _cxlumn_ids, _aliases);
}

void AliasOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> AliasOperator::_on_execute() {
  /**
   * Generate the new TableCxlumnDefinitions, that is, setting the new names for the cxlumns
   */
  auto output_cxlumn_definitions = std::vector<TableCxlumnDefinition>{};
  output_cxlumn_definitions.reserve(input_table_left()->cxlumn_count());

  for (auto cxlumn_id = CxlumnID{0}; cxlumn_id < input_table_left()->cxlumn_count(); ++cxlumn_id) {
    const auto& input_cxlumn_definition = input_table_left()->cxlumn_definitions()[_cxlumn_ids[cxlumn_id]];

    output_cxlumn_definitions.emplace_back(_aliases[cxlumn_id], input_cxlumn_definition.data_type,
                                           input_cxlumn_definition.nullable);
  }

  /**
   * Generate the output table, forwarding segments from the input chunks and ordering them according to _cxlumn_ids
   */
  const auto output_table =
      std::make_shared<Table>(output_cxlumn_definitions, input_table_left()->type(),
                              input_table_left()->max_chunk_size(), input_table_left()->has_mvcc());

  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    const auto input_chunk = input_table_left()->get_chunk(chunk_id);

    auto output_segments = Segments{};
    output_segments.reserve(input_table_left()->cxlumn_count());

    for (const auto& cxlumn_id : _cxlumn_ids) {
      output_segments.emplace_back(input_chunk->get_segment(cxlumn_id));
    }

    output_table->append_chunk(output_segments, input_chunk->get_allocator(), input_chunk->access_counter());
  }

  return output_table;
}

}  // namespace opossum
