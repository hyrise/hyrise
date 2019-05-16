#include <iostream>
#include "types.hpp"

#include "boost/hana/for_each.hpp"
#include "boost/hana/integral_constant.hpp"
#include "boost/hana/zip_with.hpp"
#include <boost/hana/string.hpp>
#include <boost/hana/ext/std/array.hpp>

#include <utility>
#include <resolve_type.hpp>
#include <operators/print.hpp>
#include "benchmark_config.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"

namespace hana = boost::hana;

namespace opossum {

/**
 * Helper to build a table with a static (specified by template args `ColumnTypes`) column type layout. Keeps a vector
 * for each column and appends values to them in append_row(). Automatically creates chunks in accordance with the
 * specified chunk size.
 */
template <class... DataTypes>
// NOLINTNEXTLINE(fuchsia-trailing-return) - clang-tidy does not like the template parameter list
class TableBuilder {
public:
  // NOLINTNEXTLINE(fuchsia-trailing-return) - clang-tidy does not like the template parameter list
  template <class... ColumnDefinitions>
  TableBuilder(size_t chunk_size, const hana::tuple<ColumnDefinitions...>& column_definition_tuples, UseMvcc use_mvcc,
      size_t estimated_rows = 0)
    : _use_mvcc(use_mvcc)
    , _estimated_rows_per_chunk(std::min(estimated_rows, chunk_size)){

    // Iterate over the column types/names and create the columns.
    auto column_definitions = TableColumnDefinitions{};
    hana::fold_left(column_definition_tuples, column_definitions,
                           [](auto& definitions, auto name_and_type_and_is_nullable) -> decltype(definitions) {
                             const auto& name = name_and_type_and_is_nullable[hana::llong_c<0>];

                             using T = typename decltype(+name_and_type_and_is_nullable[hana::llong_c<1>])::type;
                             auto data_type = data_type_from_type<T>();

                             auto is_nullable = name_and_type_and_is_nullable[hana::llong_c<2>];

                             definitions.emplace_back(name, data_type, is_nullable);

                             return definitions;
                           });
    _table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, use_mvcc);

    // Reserve some space in the vectors
    hana::for_each(_data_vectors, [&](auto&& vector) { vector.reserve(_estimated_rows_per_chunk); });

    // Create the is_null_vectors only for nullable columns and reserve some space in them
    auto column_definition_tuples_and_is_null_vectors = hana::zip_with(
      [](auto& column_definition, auto& is_null_vector) {
        return hana::make_tuple(std::reference_wrapper(column_definition), std::reference_wrapper(is_null_vector));
      },
      column_definition_tuples, _is_null_vectors);

    hana::for_each(column_definition_tuples_and_is_null_vectors,
      [&](auto& column_definition_tuple_and_is_null_vector){
        const auto& column_definition = column_definition_tuple_and_is_null_vector[hana::llong_c<0>].get();
        auto& is_null_vector = column_definition_tuple_and_is_null_vector[hana::llong_c<1>].get();

        if (column_definition[hana::llong_c<2>]) {  // if column is nullable
          is_null_vector = std::vector<bool>();
          is_null_vector.value().reserve(_estimated_rows_per_chunk);
        } else {
          is_null_vector = std::nullopt;
        }
    });
  }

  std::shared_ptr<Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  void append_row(std::optional<DataTypes>&&... column_values) {
    // Create tuples ([&data_vector0, &is_null_vector0, value0], [&data_vector1, &is_null_vector1, value1], ...)
    auto vectors_and_values = hana::zip_with(
      [](auto& data_vector, auto& is_null_vector, auto&& value) {
        return hana::make_tuple(std::reference_wrapper(data_vector), std::reference_wrapper(is_null_vector),
          std::forward<decltype(value)>(value));
      },
      _data_vectors, _is_null_vectors, hana::make_tuple(std::forward<std::optional<DataTypes>>(column_values)...));

    // Add the values to their respective data vector
    hana::for_each(vectors_and_values, [](auto&& data_vector_and_is_null_vector_and_value) {
      auto& data_vector = data_vector_and_is_null_vector_and_value[hana::llong_c<0>].get();
      auto& is_null_vector = data_vector_and_is_null_vector_and_value[hana::llong_c<1>].get();
      auto& optional_value = data_vector_and_is_null_vector_and_value[hana::llong_c<2>];

      auto column_is_nullable = is_null_vector.has_value();
      auto is_null = !optional_value.has_value();

      DebugAssert(column_is_nullable || !is_null, "cannot insert null value into not-null-column");

      if(is_null) {
        data_vector.emplace_back();
        is_null_vector.value().emplace_back(true);
      } else {
        data_vector.emplace_back(std::move(optional_value.value()));
        if (column_is_nullable){
          is_null_vector.value().emplace_back(false);
        }
      }
    });

    if (_current_chunk_row_count() >= _table->max_chunk_size()) {
      _emit_chunk();
    }
  }

private:
  std::shared_ptr<Table> _table;
  UseMvcc _use_mvcc;
  size_t _estimated_rows_per_chunk;

  hana::tuple<std::vector<DataTypes>...> _data_vectors;
  // what we want: a hana::tuple containing a bool vector for each data vector
  // in other words: std::array<std::vector<bool>, sizeof...(DataTypes)> as hana::tuple
  // so we define an alias for boolean which consumes the template parameter, so we can use "..."
  template <typename ...>
  using bool_t = bool;
  hana::tuple<std::optional<std::vector<bool_t<DataTypes>>>...> _is_null_vectors;
  // TODO(pascal): use hana::optional instead of std::optional

  size_t _current_chunk_row_count() const { return _data_vectors[hana::llong_c<0>].size(); }

  void _emit_chunk() {
    Segments segments;

    auto _data_vectors_and_is_null_vectors = boost::hana::zip_with(
      [](auto& data_vector, auto& is_null_vector) {
        return boost::hana::make_tuple(std::reference_wrapper(data_vector), std::reference_wrapper(is_null_vector));
      },
      _data_vectors, _is_null_vectors);


    // Create a segment from each data vector and add it to the Chunk, then re-initialize the vector
    hana::for_each(_data_vectors_and_is_null_vectors, [&](auto&& data_vector_and_is_null_vector) {
      auto& data_vector = data_vector_and_is_null_vector[hana::llong_c<0>].get();
      auto& is_null_vector = data_vector_and_is_null_vector[hana::llong_c<1>].get();

      using T = typename std::decay_t<decltype(data_vector)>::value_type;
      // reason for nolint: clang-tidy wants this to be a forward, but that doesn't work TODO: really?
      if (is_null_vector.has_value()){
        segments.push_back(std::make_shared<ValueSegment<T>>(std::move(data_vector), std::move(is_null_vector.value())));

        is_null_vector = std::vector<bool>();
        is_null_vector.value().reserve(_estimated_rows_per_chunk);
      } else {
        segments.push_back(std::make_shared<ValueSegment<T>>(std::move(data_vector)));
      }

      data_vector = std::decay_t<decltype(data_vector)>();
      data_vector.reserve(_estimated_rows_per_chunk);
    });

    // Create initial MvccData if MVCC is enabled
    auto mvcc_data = std::shared_ptr<MvccData>{};
    if (_use_mvcc == UseMvcc::Yes) {
      mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
    }

    _table->append_chunk(segments, mvcc_data);
  }
};

} // namespace opossum

using namespace opossum;  // NOLINT

const auto names_and_nullable = hana::make_tuple(
  hana::make_tuple("a", hana::type_c<int64_t>, false),
  hana::make_tuple("b", hana::type_c<int64_t>, true));

int main() {
  auto table_builder = TableBuilder<int64_t, int64_t>(2, names_and_nullable, UseMvcc::Yes, size_t{3});
  table_builder.append_row(11, {});
  table_builder.append_row(12, 22);
  table_builder.append_row(13, {});
  auto table = table_builder.finish_table();
  Print::print(table);
}
