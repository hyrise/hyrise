#pragma once

#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "boost/hana/assert.hpp"
#include "boost/hana/for_each.hpp"
#include "boost/hana/zip_with.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace hana = boost::hana;

namespace {
template <typename T, typename Enable = void>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<T, std::enable_if_t<std::is_same_v<T, std::optional<typename T::value_type>>>> : std::true_type {};

// is_optional_v<T> <=> T is of type std::optional<?>
template <typename T>
constexpr bool is_optional_v = is_optional<T>::value;

// get the value_type (lets call it V) of std::optional<V> and V using the same syntax - needed for templating
template <typename T, typename Enable = void>
struct GetValueType {
  using value_type = T;
};

template <typename T>
struct GetValueType<T, std::enable_if_t<is_optional_v<T>>> {
  using value_type = typename T::value_type;
};

template <typename T>
using get_value_type = typename GetValueType<T>::value_type;

// check if std::optional<V> or V is null using the same syntax - needed for templating
template <typename T, typename std::enable_if_t<!is_optional_v<T>, int> = 0>
constexpr bool is_null(const T&) {
  return false;
}

template <typename T, typename std::enable_if_t<is_optional_v<T>, int> = 0>
bool is_null(const T& optional) {
  return !optional.has_value();
}

// get the value of std::optional<V> or V using the same syntax - needed for templating
template <typename T, typename std::enable_if_t<!is_optional_v<T>, int> = 0>
get_value_type<T>& get_value(T& value) {
  return value;
}

template <typename T, typename std::enable_if_t<is_optional_v<T>, int> = 0>
get_value_type<T>& get_value(T& optional) {
  return optional.value();
}
}  // namespace

namespace opossum {

/**
 * Helper to build a table with a static (specified by template args `ColumnDefinitions`) column type layout. Keeps a
 * data vector for each column and appends values to them in append_row(). For nullable columns an additional
 * is_null_vector is kept. Automatically creates chunks in accordance with the specified chunk size.
 */
template <typename... DataTypes>
class TableBuilder {
 public:
  // names is a list of strings defining column names, types is a list of equal length defining respective column types
  template <typename... Names>
  TableBuilder(size_t chunk_size, const hana::tuple<DataTypes...>& types, const hana::tuple<Names...>& names,
               opossum::UseMvcc use_mvcc, size_t estimated_rows = 0)
      : _use_mvcc(use_mvcc), _estimated_rows_per_chunk(std::min(estimated_rows, chunk_size)) {
    BOOST_HANA_CONSTANT_ASSERT(hana::size(names) == hana::size(types));

    // Iterate over the column types/names and create the columns.
    auto column_definitions = TableColumnDefinitions{};
    hana::fold_left(hana::zip(names, types), column_definitions,
                    [](auto& definitions, const auto& name_and_type) -> decltype(definitions) {
                      auto name = name_and_type[hana::llong_c<0>];
                      auto type = name_and_type[hana::llong_c<1>];

                      auto data_type = data_type_from_type<get_value_type<decltype(type)>>();
                      auto is_nullable = is_optional<decltype(type)>();

                      definitions.emplace_back(name, data_type, is_nullable);

                      return definitions;
                    });
    _table = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size, use_mvcc);

    // Reserve some space in the vectors
    hana::for_each(_data_vectors, [&](auto&& vector) { vector.reserve(_estimated_rows_per_chunk); });

    // Create the is_null_vectors only for nullable columns and reserve some space in them
    auto types_and_is_null_vectors = hana::zip_with(
        [](auto type, auto& is_null_vector) { return hana::make_tuple(type, std::reference_wrapper(is_null_vector)); },
        types, _is_null_vectors);

    hana::for_each(types_and_is_null_vectors, [&](auto& types_and_is_null_vector) {
      auto type = types_and_is_null_vector[hana::llong_c<0>];
      auto& is_null_vector = types_and_is_null_vector[hana::llong_c<1>].get();

      hana::eval_if(is_optional<decltype(type)>(),
                    [&] {  // if column is nullable
                      is_null_vector = std::vector<bool>();
                      is_null_vector.value().reserve(_estimated_rows_per_chunk);
                    },
                    [&] {  // else
                      is_null_vector = std::nullopt;
                    });
    });
  }

  std::shared_ptr<Table> finish_table() {
    if (_current_chunk_row_count() > 0) {
      _emit_chunk();
    }

    return _table;
  }

  // Types == DataTypes, we deduce again to get universal references, so we can call with lvalues
  template<typename... Types>
  void append_row(Types&&... values) {
    auto values_tuple = hana::make_tuple(std::forward<decltype(values)>(values)...);
    BOOST_HANA_CONSTANT_ASSERT(hana::size(hana::tuple<DataTypes...>()) == hana::size(values_tuple));

    // Create tuples ([&data_vector0, &is_null_vector0, value0], [&data_vector1, &is_null_vector1, value1], ...)
    auto data_vectors_and_is_null_vectors_and_values = hana::zip_with(
        [](auto& data_vector, auto& is_null_vector, auto&& value) {
          return hana::make_tuple(std::reference_wrapper(data_vector), std::reference_wrapper(is_null_vector),
                                  std::forward<decltype(value)>(value));
        },
        _data_vectors, _is_null_vectors, values_tuple);

    // Add the values to their respective data vector
    hana::for_each(data_vectors_and_is_null_vectors_and_values, [](auto&& data_vector_and_is_null_vector_and_value) {
      auto& data_vector = data_vector_and_is_null_vector_and_value[hana::llong_c<0>].get();
      auto& is_null_vector = data_vector_and_is_null_vector_and_value[hana::llong_c<1>].get();
      auto& maybe_optional_value = data_vector_and_is_null_vector_and_value[hana::llong_c<2>];

      auto column_is_nullable = is_null_vector.has_value();
      auto value_is_null = is_null(maybe_optional_value);

      DebugAssert(column_is_nullable || !value_is_null, "cannot insert null value into not-null-column");

      if (value_is_null) {
        data_vector.emplace_back();
        is_null_vector.value().emplace_back(true);
      } else {
        // on failure: make sure append_row is called with the same types that were passed to table_builder constructor
        data_vector.emplace_back(std::move(get_value(maybe_optional_value)));
        if (column_is_nullable) {
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

  hana::tuple<std::vector<get_value_type<DataTypes>>...> _data_vectors;
  // what we want: a hana::tuple containing a bool vector for each data vector
  // in other words: std::array<std::vector<bool>, sizeof...(DataTypes)> as hana::tuple
  // so we define an alias for boolean which consumes the template parameter, so we can use "..."
  template <typename...>
  using bool_t = bool;
  hana::tuple<std::optional<std::vector<bool_t<DataTypes>>>...> _is_null_vectors;

  size_t _current_chunk_row_count() const { return _data_vectors[hana::llong_c<0>].size(); }

  void _emit_chunk() {
    Segments segments;

    auto _data_vectors_and_is_null_vectors = hana::zip_with(
        [](auto& data_vector, auto& is_null_vector) {
          return hana::make_tuple(std::reference_wrapper(data_vector), std::reference_wrapper(is_null_vector));
        },
        _data_vectors, _is_null_vectors);

    // Create a segment from each data vector and add it to the Chunk, then re-initialize the vector
    hana::for_each(_data_vectors_and_is_null_vectors, [&](auto&& data_vector_and_is_null_vector) {
      auto& data_vector = data_vector_and_is_null_vector[hana::llong_c<0>].get();
      auto& is_null_vector = data_vector_and_is_null_vector[hana::llong_c<1>].get();

      using T = typename std::decay_t<decltype(data_vector)>::value_type;
      if (is_null_vector.has_value()) {
        segments.push_back(
            std::make_shared<ValueSegment<T>>(std::move(data_vector), std::move(is_null_vector.value())));

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

}  // namespace opossum
