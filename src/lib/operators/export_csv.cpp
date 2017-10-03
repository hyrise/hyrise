#include "export_csv.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "import_export/csv.hpp"
#include "import_export/csv_writer.hpp"
#include "storage/base_attribute_vector.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"

#include "resolve_type.hpp"

namespace opossum {

ExportCsv::ExportCsv(const std::shared_ptr<const AbstractOperator> in, const std::string &filename)
    : AbstractReadOnlyOperator(in), _filename(filename) {}

const std::string ExportCsv::name() const { return "ExportCSV"; }
uint8_t ExportCsv::num_in_tables() const { return 1; }
uint8_t ExportCsv::num_out_tables() const { return 1; }

std::shared_ptr<const Table> ExportCsv::_on_execute() {
  CsvConfig config{};
  _generate_meta_info_file(_input_left->get_output(), _filename + config.meta_file_extension);
  _generate_content_file(_input_left->get_output(), _filename);

  return _input_left->get_output();
}

void ExportCsv::_generate_meta_info_file(const std::shared_ptr<const Table> &table, const std::string &meta_file) {
  CsvWriter writer(meta_file);
  // Write header line
  writer.write_line({"PropertyType", "Key", "Value"});

  // Chunk size
  writer.write_line({"ChunkSize", "", static_cast<int64_t>(table->chunk_size())});

  // Column Types
  for (ColumnID col_id{0}; col_id < table->col_count(); ++col_id) {
    writer.write_line({"ColumnType", table->column_name(col_id), table->column_type(col_id)});
  }
}

void ExportCsv::_generate_content_file(const std::shared_ptr<const Table> &table, const std::string &csv_file) {
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
  std::vector<std::shared_ptr<ColumnVisitable>> visitors(table->col_count());
  for (ColumnID col_id{0}; col_id < table->col_count(); ++col_id) {
    auto visitor = make_shared_by_column_type<ColumnVisitable, ExportCsvVisitor>(table->column_type(col_id));
    visitors[col_id] = std::move(visitor);
  }

  auto context = std::make_shared<ExportCsvContext>(writer);

  /* Multiple rows containing the values of each respective row are written.
   * Therefore we first iterate through the chunks, then through the rows in the chunks
   * and afterwards through the columns of the chunks.
   * This is a lot of iterating, but to convert a column-based table to a row-based representation
   * takes some effort.
   */
  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    auto &chunk = table->get_chunk(chunk_id);
    for (ChunkOffset row = 0; row < chunk.size(); ++row) {
      context->currentRow = row;
      for (ColumnID col_id{0}; col_id < table->col_count(); ++col_id) {
        chunk.get_column(col_id)->visit(*(visitors[col_id]), context);
      }
      writer.end_line();
    }
  }
}

template <typename T>
class ExportCsv::ExportCsvVisitor : public ColumnVisitable {
  void handle_value_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) final {
    auto context = std::static_pointer_cast<ExportCsv::ExportCsvContext>(base_context);
    const auto &column = static_cast<ValueColumn<T> &>(base_column);

    context->csvWriter.write(column.values()[context->currentRow]);
  }

  void handle_reference_column(ReferenceColumn &ref_column,
                               std::shared_ptr<ColumnVisitableContext> base_context) final {
    auto context = std::static_pointer_cast<ExportCsv::ExportCsvContext>(base_context);

    context->csvWriter.write(ref_column[context->currentRow]);
  }

  void handle_dictionary_column(BaseColumn &base_column, std::shared_ptr<ColumnVisitableContext> base_context) final {
    auto context = std::static_pointer_cast<ExportCsv::ExportCsvContext>(base_context);
    const auto &column = static_cast<DictionaryColumn<T> &>(base_column);

    context->csvWriter.write((*column.dictionary())[(column.attribute_vector()->get(context->currentRow))]);
  }
};

}  // namespace opossum
