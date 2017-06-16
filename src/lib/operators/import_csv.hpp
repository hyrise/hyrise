#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

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
   * @param buffer_size Specifies the amount of data from the input file in bytes that a single task should work on.
   * @param rfc_mode    If true parse according to RFC 4180 else parse as non-RFC format
   */
  explicit ImportCsv(const std::string& filename, const optional<std::string> tablename = nullopt, bool rfc_mode = true,
                     size_t buffer_size = 50 * 1024 * 1024 /*50 MB*/);

  // cannot move-assign because of const members
  ImportCsv& operator=(ImportCsv&&) = delete;

  // Returns the table that was created from the csv file.
  std::shared_ptr<const Table> on_execute() override;

  // Name of the operator is "ImportCSV"
  const std::string name() const override;

  // This operator has no input
  uint8_t num_in_tables() const override;

  // This operator has one table as output.
  uint8_t num_out_tables() const override;

 protected:
  // Path to the input file
  const std::string _filename;
  // Name for adding the table to the StorageManager
  const optional<std::string> _tablename;
  // Parsing mode
  const bool _rfc_mode;
  // Number of bytes that a task processes from the input file.
  const size_t _buffer_size;
};
}  // namespace opossum
