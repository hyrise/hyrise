#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "csv_meta.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/**
 * With the CsvWriter, selected tables of a database
 * can be exported to csv files.
 *
 * Additionally to the main csv file, which contains the contents of the table,
 * a meta file is generated. This meta file contains further information,
 * such as the types of the columns in the table.
 */
class CsvWriter {
 public:
  /*
   * Executes the export process.
   * @param table     The table to be written.
   * @param filename  The file to output the csv to.
   * @param config    Optional. A config.
   *
   * During this process, two files are created: <table_name>.csv and <table_name>.csv.meta
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
   *   NULL   | Kekse                 |  5.0
   *
   * The generated files will look the following:
   *
   *  example.csv
   *
   *  a,b,c
   *  1,"Hallo Welt",3.5
   *  102,"Du sagtest:""Hi!""",4.0
   *  ,"Kekse",5.0
   *
   *  example.csv.json:
   *
   *  {
   *    "columns": [
   *      {
   *        "name": "a",
   *        "nullable": true,
   *        "type": "int"
   *      },
   *      {
   *        "name": "b",
   *        "nullable": false,
   *        "type": "string"
   *      },
   *      {
   *        "name": "c",
   *        "nullable": false,
   *        "type": "float"
   *      }
   *    ]
   *  }
   *
   *
   *  which resembles the following table of meta data:
   *
   *  PropertyType  | Key | Value
   *  ------------------------------
   *  ColumnType    |  a  | int_null
   *  ColumnType    |  b  | string
   *  ColumnType    |  c  | float
   *
   */
  static void write(const Table& table, const std::string& filename, const ParseConfig& config = {});

  /*
   * Ends a row of entries in the csv file.
   */
  void end_line();

 protected:
  static void _generate_meta_info_file(const Table& table, const std::string& filename);
  static void _generate_content_file(const Table& table, const std::string& filename, const ParseConfig& config);
  static void _write(const AllTypeVariant& value, std::ofstream& ofstream, const ParseConfig& config);
  static pmr_string _escape(const pmr_string& string, const ParseConfig& config);
  static void _write_string_value(const pmr_string& value, std::ofstream& ofstream, const ParseConfig& config);
};

}  // namespace opossum
