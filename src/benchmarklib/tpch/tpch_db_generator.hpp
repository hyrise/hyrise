#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/hana/for_each.hpp"
#include "boost/hana/integral_constant.hpp"
#include "boost/hana/zip_with.hpp"

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"

namespace opossum {

class Chunk;
class Table;

template <typename... ColumnTypes>
class TableBuilder {
 public:
  template <typename... Strings>
  explicit TableBuilder(size_t chunk_size, boost::hana::tuple<Strings...> column_names) {
    _table = std::make_shared<Table>(chunk_size);

    boost::hana::zip_with(
        [&](auto column_type, auto column_name) {
          _table->add_column_definition(column_name, data_type_from_type<decltype(column_type)>());
          return 0;
        },
        boost::hana::tuple<ColumnTypes...>(), column_names);
  }

  std::shared_ptr<Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  void append_row(ColumnTypes&&... column_values) {
    boost::hana::zip_with(
        [](auto& vector, auto&& value) {
          vector.push_back(value);
          return 0;
        },
        _column_vectors, boost::hana::make_tuple(std::forward<ColumnTypes>(column_values)...));

    if (_current_chunk_row_count() >= _table->max_chunk_size()) {
      _emit_chunk();
    }
  }

 private:
  std::shared_ptr<Table> _table;
  boost::hana::tuple<pmr_concurrent_vector<ColumnTypes>...> _column_vectors;

  size_t _current_chunk_row_count() const { return _column_vectors[boost::hana::llong_c<0>].size(); }

  void _emit_chunk() {
    Chunk chunk;

    hana::for_each(_column_vectors, [&](auto&& vector) {
      using T = typename std::decay_t<decltype(vector)>::value_type;
      chunk.add_column(std::make_shared<ValueColumn<T>>(std::move(vector)));
      vector = typename std::decay_t<decltype(vector)>();
    });
    _table->emplace_chunk(std::move(chunk));
  }
};

enum class TpchTable { Part, PartSupplier, Supplier, Customer, Order, LineItem, Nation, Region };

extern std::unordered_map<TpchTable, std::string> tpch_table_names;

/**
 * NOT thread safe (internal malloc races)
 */
class TpchDbGenerator final {
 public:
  explicit TpchDbGenerator(float scale_factor, uint32_t chunk_size = 0);

  std::unordered_map<TpchTable, std::shared_ptr<Table>> generate();
  void generate_and_store();

 private:
  float _scale_factor;
  size_t _chunk_size;

  void _row_start();
  void _row_stop(TpchTable table);
};
}