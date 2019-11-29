#pragma once

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/base_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"

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
  explicit Import(const std::string& filename, const std::string& tablename, const hsql::ImportType);

  std::shared_ptr<const Table> _on_execute() final;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // Returns the name of the operator
  const std::string& name() const final;

 private:
  // Name of the import file
  const std::string _filename;
  // Name for adding the table to the StorageManager
  const std::string _tablename;
  const hsql::ImportType _type;
};

}  // namespace opossum
