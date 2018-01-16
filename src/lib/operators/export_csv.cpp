#include "export_csv.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "import_export/csv_meta.hpp"
#include "import_export/csv_writer.hpp"
#include "json.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"

#include "constant_mappings.hpp"
#include "resolve_type.hpp"

namespace opossum {

ExportCsv::ExportCsv(const std::shared_ptr<const AbstractOperator> in, const std::string& filename)
    : AbstractReadOnlyOperator(in), _filename(filename) {}

const std::string ExportCsv::name() const { return "ExportCSV"; }

std::shared_ptr<const Table> ExportCsv::_on_execute() {
  _generate_meta_info_file(_input_left->get_output(), _filename + CsvMeta::META_FILE_EXTENSION);
  _generate_content_file(_input_left->get_output(), _filename);

  return _input_left->get_output();
}

void ExportCsv::_generate_meta_info_file(const std::shared_ptr<const Table>& table, const std::string& meta_file_path) {
  CsvMeta meta{};
  meta.chunk_size = table->max_chunk_size();

  // Column Types
  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    ColumnMeta column_meta;
    column_meta.name = table->column_name(column_id);
    column_meta.type = data_type_to_string.left.at(table->column_type(column_id));
    column_meta.nullable = table->column_is_nullable(column_id);

    meta.columns.push_back(column_meta);
  }

  nlohmann::json meta_json = meta;

  std::ofstream meta_file_stream(meta_file_path);
  meta_file_stream << std::setw(4) << meta_json << std::endl;
}

void ExportCsv::_generate_content_file(const std::shared_ptr<const Table>& table, const std::string& csv_file) {
  /*
   * A naively exported csv file is a materialized file in row format.
   * This offers some advantages, but also disadvantages.
   * The advantages are that it is very straight forward to implement for any column type
   * as it does not care about representation of values. Also, probably, the main reason for this,
   * it makes is very easy to load this data into a different database.
   * The disadvantage is that it can be quite slow if the data has been compressed before.
   * Also, it does not involve the column-oriented style used in OpossumDB.
   */

  // Open file for writing
  CsvWriter writer(csv_file);

  // Create visitors for every column, so that we do not have to do that more than once.
  std::vector<std::shared_ptr<ColumnVisitable>> visitors(table->column_count());
  for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
    auto visitor = make_shared_by_data_type<ColumnVisitable, ExportCsvVisitor>(table->column_type(column_id));
    visitors[column_id] = std::move(visitor);
  }

  auto context = std::make_shared<ExportCsvContext>(writer);

  /* Multiple rows containing the values of each respective row are written.
   * Therefore we first iterate through the chunks, then through the rows in the chunks
   * and afterwards through the columns of the chunks.
   * This is a lot of iterating, but to convert a column-based table to a row-based representation
   * takes some effort.
   */
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    for (ChunkOffset row = 0; row < chunk->size(); ++row) {
      context->current_row = row;
      for (ColumnID column_id{0}; column_id < table->column_count(); ++column_id) {
        chunk->get_column(column_id)->visit(*(visitors[column_id]), context);
      }
      writer.end_line();
    }
  }
}

template <typename T>
class ExportCsv::ExportCsvVisitor : public ColumnVisitable {
  void handle_value_column(const BaseValueColumn& base_column,
                           std::shared_ptr<ColumnVisitableContext> base_context) final {
    auto context = std::static_pointer_cast<ExportCsv::ExportCsvContext>(base_context);
    const auto& column = static_cast<const ValueColumn<T>&>(base_column);

    auto row = context->current_row;

    if (column.is_nullable() && column.null_values()[row]) {
      // Write an empty field for a null value
      context->csv_writer.write("");
    } else {
      context->csv_writer.write(column.values()[row]);
    }
  }

  void handle_reference_column(const ReferenceColumn& ref_column,
                               std::shared_ptr<ColumnVisitableContext> base_context) final {
    auto context = std::static_pointer_cast<ExportCsv::ExportCsvContext>(base_context);

    context->csv_writer.write(ref_column[context->current_row]);
  }

  void handle_dictionary_column(const BaseDictionaryColumn& base_column,
                                std::shared_ptr<ColumnVisitableContext> base_context) final {
    auto context = std::static_pointer_cast<ExportCsv::ExportCsvContext>(base_context);
    const auto& column = static_cast<const DictionaryColumn<T>&>(base_column);

    context->csv_writer.write((*column.dictionary())[(column.attribute_vector()->get(context->current_row))]);
  }
};

}  // namespace opossum
