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
 * This operator reads a file, creates a table from that input and adds it to the storage manager.
 * Supported file types are .tbl, .csv and Opossum .bin files.
 * For .csv files, a CSV config is additionally required, which is commonly located in the <filename>.json file.
 * Documentation of the file formats can be found in BinaryWriter and CsvWriter header files.
 */
class Import : public AbstractReadOnlyOperator {
 public:
  /**
   * @param filename       Path to the input file.
   * @param tablename      Name of the table to store in the StorageManager.
   * @param chunk_size     Optional. Chunk size. Does not effect binary import.
   * @param file_type      Optional. Type indicating the file format. If not present, it is guessed by the filename.
   * @param csv_meta       Optional. A specific meta config, used instead of filename + '.json'
   */
  explicit Import(const std::string& init_filename, const std::string& tablename,
                  const ChunkOffset chunk_size = Chunk::DEFAULT_SIZE, const FileType file_type = FileType::Auto,
                  const std::optional<CsvMeta>& csv_meta = std::nullopt);

  const std::string& name() const final;
  const std::string filename;

 protected:
  std::shared_ptr<const Table> _on_execute() final;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  // Name for adding the table to the StorageManager
  const std::string _tablename;
  const ChunkOffset _chunk_size;
  FileType _file_type;
  const std::optional<CsvMeta> _csv_meta;
};

}  // namespace opossum
