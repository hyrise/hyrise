#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "import_export/csv_meta.hpp"
#include "utils/assert.hpp"

namespace opossum {

/*
 * Creates a Table with values of to the parsed csv file <filename> and the corresponding meta file
 * <filename>.meta
 * For the structure of the meta csv file see export_csv.hpp
 * If parameter tablename provided, the imported table is stored in the StorageManager. If a table with this name
 * already exists, it is returned and no import is performed.
 *
 * Note: ImportCsv does not support null values at the moment
 */
class ImportCsv : public AbstractReadOnlyOperator {
 public:
  /*
   * @param filename    Path to the input file.
   * @param tablename   Optional. Name of the table to store/look up in the StorageManager.
   */
  explicit ImportCsv(const std::string& filename, const std::optional<std::string> tablename = std::nullopt);

  /*
   * @param filename    Path to the input file.
   * @param meta        A specific meta config, to override the given .json file.
   * @param tablename   Optional. Name of the table to store/look up in the StorageManager.
   */
  explicit ImportCsv(const std::string& filename, const CsvMeta& meta,
                     const std::optional<std::string> tablename = std::nullopt);

  // cannot move-assign because of const members
  ImportCsv& operator=(ImportCsv&&) = delete;

  // Name of the operator is "ImportCSV"
  const std::string name() const override;

 protected:
  // Returns the table that was created from the csv file.
  std::shared_ptr<const Table> _on_execute() override;

  // Path to the input file
  const std::string _filename;
  // Name for adding the table to the StorageManager
  const std::optional<std::string> _tablename;
  // CSV meta information
  CsvMeta _meta;
  // Information whether custom meta information are provided (true) or the meta file should be used
  bool _custom_meta;
};
}  // namespace opossum
