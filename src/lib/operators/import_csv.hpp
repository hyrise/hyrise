#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"

namespace opossum {

/*
 * Creates a Table with values of to the parsed csv file <filename>.csv and the corresponding meta file
 * <filename>.meta.csv
 * The files are parsed according to RFC 4180.
 * For the structure of the meta csv file see export_csv.hpp
 * If parameter tablename provided, the imported table is stored in the StorageManager. If a table with this name
 * already exists, it is returned and no import is performed.
 */
class ImportCsv : public AbstractReadOnlyOperator {
 public:
  /*
   * @param directory   The directory where the csv files are located in.
   * @param filename    Name of the input file. The operator opens the files <filename>.csv and <filename>.meta.csv
   * @param tablename   Optional. Name of the table to store/look up in the StorageManager.
   */
  explicit ImportCsv(const std::string& directory, const std::string& filename,
                     const std::string& tablename = std::string());

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
  // Path to the directory containing the csv file
  const std::string _directory;
  // Name of the csv file without file extension
  const std::string _filename;
  // Name for adding the table to the StorageManager
  const optional<std::string> _tablename;

  /* Creates the table structure from the meta file.
  *  The following is the content an example meta file:
  *
  *  PropertyType,Key,Value
  *  Chunk Size,,100
  *  Column Type,a,int
  *  Column Type,b,string
  *  Column Type,c,float
  */
  static const std::shared_ptr<Table> _process_meta_file(const std::string& meta_file);
  // Converts given string value to given type
  static AllTypeVariant _convert(const std::string& value, const std::string& type);
  // Removes all csv escaping characters
  static std::string _unescape(const std::string& input);
  // Reads until a given delimiter.
  // This function does not stop at escaped delimiters.
  static bool _read_csv(std::istream& stream, std::string& out, char delimiter);
  // Reads in a full CSV row
  static bool _get_row(std::istream& stream, std::string& out);
  // Reads in a full CSV field
  static bool _get_field(std::istream& stream, std::string& out);
  // Splits and returns all fields from a given CSV row.
  static std::vector<std::string> _get_fields(const std::string& row);
};
}  // namespace opossum
