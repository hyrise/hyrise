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
  return std::make_shared<AliasOperator>(copied_input_left, _column_ids, _aliases);
}

void AliasOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> AliasOperator::_on_execute() {
  /**
   * Generate the new TableColumnDefinitions, that is, setting the new names for the columns
   */
  auto output_column_definitions = std::vector<TableColumnDefinition>{};
  output_column_definitions.reserve(input_table_left()->column_count());

  for (auto column_id = ColumnID{0}; column_id < input_table_left()->column_count(); ++column_id) {
    const auto& input_column_definition = input_table_left()->column_definitions()[_column_ids[column_id]];

    output_column_definitions.emplace_back(_aliases[column_id], input_column_definition.data_type,
                                           input_column_definition.nullable);
  }

  /**
   * Generate the output table, forwarding columns from the input chunks and ordering them according to _column_ids
   */
  const auto output_table =
      std::make_shared<Table>(output_column_definitions, input_table_left()->type(),
                              input_table_left()->max_chunk_size(), input_table_left()->has_mvcc());

  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    const auto input_chunk = input_table_left()->get_chunk(chunk_id);

    auto output_columns = ChunkColumns{};
    output_columns.reserve(input_table_left()->column_count());

    for (const auto& column_id : _column_ids) {
      output_columns.emplace_back(input_chunk->get_column(column_id));
    }

    output_table->append_chunk(output_columns, input_chunk->get_allocator(), input_chunk->access_counter());
  }

  return output_table;
}

}  // namespace opossum
