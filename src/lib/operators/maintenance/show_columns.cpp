#include "show_columns.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

ShowColumns::ShowColumns(const std::string &table_name) : _table_name(table_name) {}

uint8_t ShowColumns::num_in_tables() const { return 0; }

uint8_t ShowColumns::num_out_tables() const { return 1; }

const std::string ShowColumns::name() const { return "ShowColumns"; }

std::shared_ptr<AbstractOperator> ShowColumns::recreate(const std::vector<AllParameterVariant> &args) const {
  return std::make_shared<ShowColumns>(_table_name);
}

std::shared_ptr<const Table> ShowColumns::_on_execute() {
  auto out_table = std::make_shared<Table>();
  out_table->add_column_definition("column_name", "string");
  out_table->add_column_definition("column_type", "string");
  out_table->add_column_definition("is_nullable", "int");

  const auto table = StorageManager::get().get_table(_table_name);
  Chunk chunk;

  const auto &column_names = table->column_names();
  const auto vc_names = std::make_shared<ValueColumn<std::string>>(
      tbb::concurrent_vector<std::string>(column_names.begin(), column_names.end()));
  chunk.add_column(vc_names);

  const auto &column_types = table->column_types();
  const auto vc_types = std::make_shared<ValueColumn<std::string>>(
      tbb::concurrent_vector<std::string>(column_types.begin(), column_types.end()));
  chunk.add_column(vc_types);

  const auto &column_nullables = table->column_nullables();
  const auto vc_nullables = std::make_shared<ValueColumn<int32_t>>(
      tbb::concurrent_vector<int32_t>(column_nullables.begin(), column_nullables.end()));
  chunk.add_column(vc_nullables);

  out_table->add_chunk(std::move(chunk));

  return out_table;
}

}  // namespace opossum
