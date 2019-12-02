#pragma once

//#include <fstream>
//#include <memory>
#include <optional>
#include <string>
//#include <utility>

#include "abstract_read_only_operator.hpp"
#include "import_export/csv/csv_meta.hpp"

#include "SQLParser.h"

namespace opossum {

/*
 * This operator reads a Opossum binary file and creates a table from that input.
 * If parameter tablename provided, the imported table is stored in the StorageManager. If a table with this name
 * already exists, it is returned and no import is performed.
 *
 * Note: ImportBinary does not support null values at the moment
 */
class Import : public AbstractReadOnlyOperator {
 public:
  explicit Import(const std::string& file_name,
                  const std::optional<std::string>& table_name = std::nullopt,
                  const std::optional<hsql::ImportType>& type = std::nullopt,
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
  const std::string _file_name;
  // Name for adding the table to the StorageManager
  const std::optional<std::string> _table_name;
  const std::optional<hsql::ImportType> _type;
  const ChunkOffset _chunk_size;
  const std::optional<CsvMeta> _csv_meta;

  std::shared_ptr<Table> _import();
  std::shared_ptr<Table> _import_any_file();

  static std::shared_ptr<Table> _import_csv(const std::string& file_name, const ChunkOffset& chunk_size,
      const std::optional<CsvMeta>& csv_meta);
  static std::shared_ptr<Table> _import_tbl(const std::string& file_name, const ChunkOffset& chunk_size);
  static std::shared_ptr<Table> _import_binary(const std::string& file_name);
};

}  // namespace opossum
