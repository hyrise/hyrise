#include "generate_table_statistics.hpp"

#include <unordered_set>

#include "base_cxlumn_statistics.hpp"
#include "cxlumn_statistics.hpp"
#include "generate_cxlumn_statistics.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"

namespace opossum {

TableStatistics generate_table_statistics(const Table& table) {
  std::vector<std::shared_ptr<const BaseCxlumnStatistics>> cxlumn_statistics;
  cxlumn_statistics.reserve(table.cxlumn_count());

  for (CxlumnID cxlumn_id{0}; cxlumn_id < table.cxlumn_count(); ++cxlumn_id) {
    const auto cxlumn_data_type = table.cxlumn_data_types()[cxlumn_id];

    resolve_data_type(cxlumn_data_type, [&](auto type) {
      using CxlumnDataType = typename decltype(type)::type;
      cxlumn_statistics.emplace_back(generate_cxlumn_statistics<CxlumnDataType>(table, cxlumn_id));
    });
  }

  return {table.type(), static_cast<float>(table.row_count()), cxlumn_statistics};
}

}  // namespace opossum
