#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "import_export/file_type.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/*
 * This operator writes a table into a file.
 * Supportes file types are .csv and Hyrise .bin files.
 * For .csv files, a CSV config is added, which is located in the <filename>.json file.
 * Documentation of the file formats can be found in BinaryWriter and CsvWriter header files.
 */
class Export : public AbstractReadOnlyOperator {
 public:
  /**
   * @param in             Operator wrapping the table.
   * @param filename       Path to the output file.
   * @param file_type      Optional. Type indicating the file format. If not present, it is guessed by the filename.
   */
  explicit Export(const std::shared_ptr<const AbstractOperator>& input_operator, const std::string& filename,
                  const FileType& file_type = FileType::Auto);

  const std::string& name() const final;
  std::string description(DescriptionMode description_mode) const override;

 protected:
  /**
   * Executes the export operator
   * @return The table that was also the input
   */
  std::shared_ptr<const Table> _on_execute() final;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  // Path of the binary file
  const std::string _filename;
  FileType _file_type;
};

}  // namespace hyrise
