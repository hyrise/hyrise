#include "meta_columns_table.hpp"

#include "hyrise.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/table_statistics.hpp"

namespace hyrise {

MetaColumnsTable::MetaColumnsTable()
    : AbstractMetaTable(TableColumnDefinitions{{"table_name", DataType::String, false},
                                               {"column_name", DataType::String, false},
                                               {"data_type", DataType::String, false},
                                               {"nullable", DataType::Int, false},
                                               {"unique_value_count", DataType::Long, true},
                                               {"null_value_ratio", DataType::Float, true}}) {}

const std::string& MetaColumnsTable::name() const {
  static const auto name = std::string{"columns"};
  return name;
}

std::shared_ptr<Table> MetaColumnsTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    const auto& table_statistics = table->table_statistics();
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      auto unique_value_count = NULL_VALUE;
      auto null_value_ratio = NULL_VALUE;

      if (table_statistics) {
        const auto& column_statistics = table_statistics->column_statistics;
        Assert(column_id < column_statistics.size(), "Expected statistics object");
        resolve_data_type(table->column_data_type(column_id), [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;
          const auto& attribute_statistics =
              static_cast<AttributeStatistics<ColumnDataType>&>(*column_statistics[column_id]);
          const auto& null_value_ratio_object = attribute_statistics.null_value_ratio;
          Assert(null_value_ratio_object, "Corrupted statistics object");
          null_value_ratio = null_value_ratio_object->ratio;

          const auto& histogram = attribute_statistics.histogram;
          if (histogram) {
            unique_value_count = static_cast<int64_t>(histogram->total_distinct_count());
          }
        });
      }
      output_table->append({pmr_string{table_name}, static_cast<pmr_string>(table->column_name(column_id)),
                            static_cast<pmr_string>(data_type_to_string.left.at(table->column_data_type(column_id))),
                            static_cast<int32_t>(table->column_is_nullable(column_id)), unique_value_count,
                            null_value_ratio});
    }
  }

  return output_table;
}

}  // namespace hyrise
