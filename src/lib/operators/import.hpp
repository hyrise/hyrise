#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "abstract_read_only_operator.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/file_type.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace hyrise {

/*
 * This operator reads a file, creates a table from that input and adds it to the storage manager.
 * Supported file types are .tbl, .csv and Hyrise .bin files.
 * For .csv files, a CSV config is additionally required (if no table with the same name exists as a template).
 * The operator looks for this content in the file <filename>.json if the table does not exist.
 * If a table with this filename already exists, it uses it as the ground truth for meta information and appends.
 * Documentation of the file formats can be found in BinaryWriter and CsvWriter header files.
 */
class Import : public AbstractReadOnlyOperator {
 public:
  /**
   * @param filename       Path to the input file.
   * @param tablename      Name of the table to store in the StorageManager.
   * @param chunk_size     Optional. Chunk size. Does not effect binary import.
   * @param file_type      Optional. Type indicating the file format. If not present, it is guessed by the filename.
   * @param target_encoding Optional. Encoding of the imported table. If not present the default encoding is used.
   */
  explicit Import(const std::string& init_filename, const std::string& tablename,
                  const ChunkOffset chunk_size = Chunk::DEFAULT_SIZE, const FileType file_type = FileType::Auto,
                  const std::optional<EncodingType> target_encoding = std::nullopt,
                  const std::optional<ParseConfig>& csv_parse_config = std::nullopt);

  const std::string& name() const final;
  const std::string filename;

 protected:
  std::shared_ptr<const Table> _on_execute() final;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  // Name for adding the table to the StorageManager
  const std::string _tablename;
  const ChunkOffset _chunk_size;
  FileType _file_type;
  std::optional<ParseConfig> _csv_parse_config;
  const std::optional<EncodingType> _target_encoding;
};

}  // namespace hyrise
