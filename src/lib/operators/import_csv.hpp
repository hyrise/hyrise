#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "import_export/csv_meta.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Creates a Table with values of the parsed csv file <filename>
 * and the corresponding meta file <filename>.meta
 * For the structure of the meta csv file see export_csv.hpp
 *
 * If the parameter `tablename` is provided, the imported table is stored in the StorageManager.
 * If a table with this name already exists, it is returned and no import is performed.
 *
 * TODO(mjendruk): is this still true?
 * Note: ImportCsv does not support null values at the moment
 */
class ImportCsv : public AbstractReadOnlyOperator {
 public:
  /**
   * @param filename      Path to the input file.
   * @param tablename     Optional. Name of the table to store/look up in the StorageManager.
   * @param meta          Optional. A specific meta config, to override the given .json file.
   */
  explicit ImportCsv(const std::string& filename, const ChunkOffset chunk_size = Chunk::DEFAULT_SIZE,
                     const std::optional<std::string>& tablename = std::nullopt,
                     const std::optional<CsvMeta>& csv_meta = std::nullopt);

  const std::string& name() const override;

 protected:
  // Returns the table that was created from the csv file.
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  // Path to the input file
  const std::string _filename;

  const ChunkOffset _chunk_size;

  // Name for adding the table to the StorageManager
  const std::optional<std::string> _tablename;
  // CSV meta information
  const std::optional<CsvMeta> _csv_meta;
};
}  // namespace opossum
