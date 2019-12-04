#pragma once

#include <optional>
#include <string>

#include "abstract_read_only_operator.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/file_type.hpp"
#include "types.hpp"

#include "SQLParser.h"

namespace opossum {

/*
 * This operator reads a file and creates a table from that input.
 * Supportes file types are .tbl, .csv and Opossum .bin files.
 * For .csv files, a CSV config is additionally required, which is commonly located in the <filename>.json file.
 * If parameter tablename provided, the imported table is stored in the StorageManager. If a table with this name
 * already exists, it is returned and no import is performed.
 */
class Import : public AbstractReadOnlyOperator {
 public:
  /**
   * @param filename      Path to the input file.
   * @param tablename     Optional. Name of the table to store/look up in the StorageManager.
   * @param import_type    Optional. Type indicating the file format. If not present, it is guessed by the filename.
   * @param chunk_size     Optional. Chunk size. Does not effect binary import.
   * @param csv_meta       Optional. A specific meta config, to override the given .json file.
   */
  explicit Import(const std::string& filename, const std::optional<std::string>& tablename = std::nullopt,
                  const std::optional<FileType>& type = std::nullopt,
                  const ChunkOffset chunk_size = Chunk::DEFAULT_SIZE,
                  const std::optional<CsvMeta>& csv_meta = std::nullopt);

  // Returns the name of the operator
  const std::string& name() const final;

 protected:
  std::shared_ptr<const Table> _on_execute() final;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  // Name of the import file
  const std::string _filename;
  // Name for adding the table to the StorageManager
  const std::optional<std::string> _tablename;
  const std::optional<FileType> _type;
  const ChunkOffset _chunk_size;
  const std::optional<CsvMeta> _csv_meta;

  std::shared_ptr<Table> _import();
  std::shared_ptr<Table> _import_any_file();

  static std::shared_ptr<Table> _import_csv(const std::string& filename, const ChunkOffset& chunk_size,
                                            const std::optional<CsvMeta>& csv_meta);
  static std::shared_ptr<Table> _import_tbl(const std::string& filename, const ChunkOffset& chunk_size);
  static std::shared_ptr<Table> _import_binary(const std::string& filename);
};

}  // namespace opossum
