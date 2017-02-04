#pragma once

#include <memory>
#include <string>

#include "abstract_read_only_operator.hpp"

namespace opossum {
/*
 * With the ExportCsv operator, selected tables of a database
 * can be exported to csv files. A valid input can herefore be
 * either a table in the storagemanager or a result of a different operator.
 *
 * Additionally to the main csv file, which contains the contents of the table,
 * a meta file is generated. This meta file contains further information,
 * such as the types of the columns in the table.
 */
class ExportCsv : public AbstractReadOnlyOperator {
 public:
  /*
   * Generates a new ExportCsv operator.
   * @param in          The input for this operator. Must be another operator,
   *                    whose output is used as output for the table. If exporting
   *                    a predefined table is wished, it must first be retrieved with
   *                    the gettable operator.
   * @param directory   The directory where the files will be created.
   * @param filename    The name assigned to the table for this export. This has no other effect
   *                    than to define the file names: <filename>.csv and <filename>.meta.csv
   */
  explicit ExportCsv(const std::shared_ptr<const AbstractOperator> in, const std::string& directory,
                     const std::string& filename);

  // cannot move-assign because of const members
  ExportCsv& operator=(ExportCsv&&) = delete;

  /*
   * Executes the export process.
   * During this process, two files are created: <table_name>.csv and <table_name>.meta.csv
   * Currently, they are both csv files with a comma (,) as delimiter
   * and a quotation mark (") as quotation mark. As escape character, also a quotation mark is used (").
   * This definition is in line with RFC 4180
   *
   *
   * For explanation of the output format, consider the following example:
   * Given table, with name "example", chunk size 100:
   *  a (int) | b (string)            | c (float)
   *  -------------------------------------------
   *    1     | Hallo Welt            |  3.5
   *   102    | Du: sagtest: "Hi!"    |  4.0
   *
   * The generated files will look the following:
   *
   *  example.csv
   *
   *  a,b,c
   *  1,"Hallo Welt",3.5
   *  102,"Du sagtest:""Hi!""",4.0
   *
   *  example.meta.csv:
   *
   *  PropertyType,Key,Value
   *  Chunk Size,,100
   *  Column Type,a,int
   *  Column Type,b,string
   *  Column Type,c,float
   *
   *  which resembles the following table of meta data:
   *
   *  PropertyType  | Key | Value
   *  ---------------------------
   *  Chunk Size    |     | 100
   *  Column Type   | a   | int
   *  Column Type   | b   | string
   *  Column Type   | c   | float
   *
   *  Returns the input table
   */
  std::shared_ptr<const Table> on_execute() override;

  /*
   * Name of the operator is ExportCsv
   */
  const std::string name() const override;

  /*
   * This operator allows one table as input
   */
  uint8_t num_in_tables() const override;

  /*
   * This operator has one table as output.
   */
  uint8_t num_out_tables() const override;

 protected:
  // Path to the directory containing the csv file
  const std::string _directory;
  // Name of the csv file without file extension
  const std::string _filename;

  static void _generate_meta_info_file(const std::shared_ptr<const Table>& table, const std::string& meta_file);
  static void _generate_content_file(const std::shared_ptr<const Table>& table, const std::string& csv_file);
};
}  // namespace opossum
