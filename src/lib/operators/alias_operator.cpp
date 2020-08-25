#include "alias_operator.hpp"

#include <sstream>

#include "boost/algorithm/string/join.hpp"
#include "storage/table.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

AliasOperator::AliasOperator(const std::shared_ptr<const AbstractOperator>& input,
                             const std::vector<ColumnID>& column_ids, const std::vector<std::string>& aliases)
    : AbstractReadOnlyOperator(OperatorType::Alias, input, nullptr), _column_ids(column_ids), _aliases(aliases) {
  Assert(_column_ids.size() == _aliases.size(), "Expected as many aliases as columns");
}

const std::string& AliasOperator::name() const {
  static const auto name = std::string{"Alias"};
  return name;
}

std::string AliasOperator::description(DescriptionMode description_mode) const {
  const auto* const separator = description_mode == DescriptionMode::SingleLine ? " " : "\n";
  std::stringstream stream;

  stream << "Alias" << separator << "[";
  stream << boost::algorithm::join(_aliases, ", ");
  stream << "]";
  return stream.str();
}

std::shared_ptr<AbstractOperator> AliasOperator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<AliasOperator>(copied_left_input, _column_ids, _aliases);
}

void AliasOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> AliasOperator::_on_execute() {
  const auto& input_table = *left_input_table();

  /**
   * Generate the new TableColumnDefinitions, that is, setting the new names for the columns
   */
  auto output_column_definitions = std::vector<TableColumnDefinition>{};
  output_column_definitions.reserve(input_table.column_count());

  for (auto column_id = ColumnID{0}; column_id < input_table.column_count(); ++column_id) {
    const auto& input_column_definition = input_table.column_definitions()[_column_ids[column_id]];

    output_column_definitions.emplace_back(_aliases[column_id], input_column_definition.data_type,
                                           input_column_definition.nullable);
  }

  /**
   * Generate the output table, forwarding segments from the input chunks and ordering them according to _column_ids
   */
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{input_table.chunk_count()};

  const auto chunk_count = input_table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    auto output_segments = Segments{};
    output_segments.reserve(input_table.column_count());

    for (const auto& column_id : _column_ids) {
      output_segments.emplace_back(input_chunk->get_segment(column_id));
    }

    auto output_chunk = std::make_shared<Chunk>(std::move(output_segments), input_chunk->mvcc_data());
    output_chunk->finalize();
    // The alias operator does not affect sorted_by property. If a chunk was sorted before, it still is after.
    const auto& sorted_by = input_chunk->individually_sorted_by();
    if (!sorted_by.empty()) {
      auto sort_definitions = std::vector<SortColumnDefinition>{};
      sort_definitions.reserve(sorted_by.size());

      // Adapt column ids
      for (const auto& sort_definition : sorted_by) {
        const auto it = std::find(_column_ids.cbegin(), _column_ids.cend(), sort_definition.column);
        Assert(it != _column_ids.cend(), "Chunk is sorted by an invalid column ID.");
        const auto index = ColumnID{static_cast<uint16_t>(std::distance(_column_ids.cbegin(), it))};
        sort_definitions.emplace_back(SortColumnDefinition(index, sort_definition.sort_mode));
      }

      output_chunk->set_individually_sorted_by(sort_definitions);
    }
    output_chunks[chunk_id] = output_chunk;
  }

  return std::make_shared<Table>(output_column_definitions, input_table.type(), std::move(output_chunks),
                                 input_table.uses_mvcc());
}

}  // namespace opossum
