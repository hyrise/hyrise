#include "import.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "import_export/file_type.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/encoding_type.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

Import::Import(const std::string& init_filename, const std::string& tablename, const ChunkOffset chunk_size,
               const FileType file_type, const std::optional<EncodingType> target_encoding)
    : AbstractReadOnlyOperator(OperatorType::Import),
      filename(init_filename),
      _tablename(tablename),
      _chunk_size(chunk_size),
      _file_type(file_type),
      _target_encoding(target_encoding) {
  if (_file_type == FileType::Auto) {
    _file_type = file_type_from_filename(filename);
  }
}

const std::string& Import::name() const {
  static const auto name = std::string{"Import"};
  return name;
}

std::shared_ptr<const Table> Import::_on_execute() {
  // Check if file exists before giving it to the parser
  auto file = std::ifstream{filename};
  Assert(file.is_open(), "Import: Could not find file " + filename);
  file.close();

  auto table = std::shared_ptr<Table>{};

  switch (_file_type) {
    case FileType::Csv: {
      auto csv_meta = CsvMeta{};
      const auto meta_filename = filename + CsvMeta::META_FILE_EXTENSION;
      const auto meta_file_exists = std::filesystem::exists(meta_filename);
      const auto table_exists = Hyrise::get().storage_manager.has_table(_tablename);
      if (table_exists) {
        if (meta_file_exists) {
          std::cerr << "Warning: Ignoring " << meta_filename << " because table " << _tablename << " already exists.\n";
        }

        const auto& column_definitions = Hyrise::get().storage_manager.get_table(_tablename)->column_definitions();
        const auto column_count = column_definitions.size();

        csv_meta.columns.resize(column_count);
        for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
          csv_meta.columns[column_id].name = column_definitions[column_id].name;
          csv_meta.columns[column_id].type = data_type_to_string.left.at(column_definitions[column_id].data_type);
          csv_meta.columns[column_id].nullable = column_definitions[column_id].nullable;
        }
      } else if (meta_file_exists) {
        csv_meta = process_csv_meta_file(meta_filename);
      } else {
        Fail("Cannot load table from csv. No table definition source found.");
      }

      table = CsvParser::parse(filename, csv_meta, _chunk_size);
      break;
    }
    case FileType::Tbl: {
      table = load_table(filename, _chunk_size);
      break;
    }
    case FileType::Binary: {
      table = BinaryParser::parse(filename);
      break;
    }
    case FileType::Auto: {
      Fail("File type should have been determined previously.");
    }
  }

  if (Hyrise::get().storage_manager.has_table(_tablename)) {
    Hyrise::get().storage_manager.drop_table(_tablename);
  }

  Hyrise::get().storage_manager.add_table(_tablename, table);

  // For binary files, the default is the encoding of the file.
  if (_file_type != FileType::Binary) {
    auto chunk_encoding_spec = ChunkEncodingSpec{};

    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      // If a target encoding is specified and supported, use it. Otherwise, select the encoding automatically.
      const auto& column_data_type = table->column_data_type(column_id);
      if (_target_encoding && encoding_supports_data_type(*_target_encoding, column_data_type)) {
        chunk_encoding_spec.emplace_back(*_target_encoding);
      } else {
        const auto segment_values_are_unique = column_is_unique(table, column_id);
        const auto segment_values_are_key_part = column_is_key_part(table, column_id);
        chunk_encoding_spec.emplace_back(auto_select_segment_encoding_spec(column_data_type, segment_values_are_unique,
                                                                           segment_values_are_key_part));
      }
    }
    ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
  }

  if (!Hyrise::get().storage_manager.has_table(_tablename)) {
    // We create statistics when tables are added to the storage manager. As statistics can be expensive to create
    // and their creation benefits from dictionary encoding, we add the tables after they are encoded.
    Hyrise::get().storage_manager.add_table(_tablename, table);
    return nullptr;
  }

  const auto existing_table = Hyrise::get().storage_manager.get_table(_tablename);
  const auto append_lock = existing_table->acquire_append_mutex();
  if (existing_table->chunk_count() > 0 && existing_table->last_chunk()->is_mutable()) {
    existing_table->last_chunk()->set_immutable();
  }

  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    existing_table->append_chunk(chunk->segments(), chunk->mvcc_data());
  }

  table->set_table_statistics(TableStatistics::from_table(*table));
  generate_chunk_pruning_statistics(table);

  // We must match ImportNode::output_expressions.
  return nullptr;
}

std::shared_ptr<AbstractOperator> Import::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Import>(filename, _tablename, _chunk_size, _file_type, _target_encoding);
}

void Import::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace hyrise
