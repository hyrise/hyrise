#pragma once

#include <boost/hana/for_each.hpp>

#include <string>

#include "all_type_variant.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * Resolves a column type by passing a hana::type object on to a generic lambda
 *
 * @param type is a string representation of any of the supported column types
 * @param func is a generic lambda or similar accepting a hana::type object
 *
 *
 * Example:
 *
 *   template <typename T>
 *   consume_column_v1();
 *
 *   template <typename T>
 *   consume_column_v2(hana::basic_type<T> type);  // note: parameter type needs to be hana::basic_type not hana::type!
 *
 *   resolve_column_type(column_type, base_column, [&] (auto type) {
 *     using Type = typename decltype(type)::type;
 *     consume_column_v1<Type>();
 *
 *     consume_column_v2(type);  // here you don’t have to retrieve the template type `Type`
 *   });
 */
template <typename Functor>
void resolve_type(const std::string &type, const Functor &func) {
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      // The + before hana::second - which returns a reference - converts its return value into a value
      func(+hana::second(x));
      return;
    }
  });
}

/**
 * Resolves a column type by passing a hana::type object and the resolved column on to a generic lambda
 *
 * @param type is a string representation of any of the supported column types
 * @param func is a generic lambda or similar accepting two parameters: a hana::type object and
 *   a reference to a specialized column (value, dictionary, reference)
 *
 *
 * Example:
 *   template <typename T>
 *   consume_column(BaseColumn &column);
 *
 *   resolve_column_type(column_type, base_column, [&] (auto type, auto &typed_column) {
 *     using Type = typename decltype(type)::type;
 *     using ColumnType = typename std::decay<decltype(typed_column)>::type;
 *
 *     constexpr auto is_reference_column = (std::is_same<ColumnType, ReferenceColumn>{});
 *     constexpr auto is_string_column = (std::is_same<Type, std::string>{});
 *
 *     consume_column<Type>(typed_column);
 *   });
 */
template <typename Functor>
void resolve_column_type(const std::string &type, BaseColumn &column, const Functor &func) {
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      const auto type = hana::second(x);
      using Type = typename decltype(type)::type;

      if (auto value_column = dynamic_cast<ValueColumn<Type> *>(&column)) {
        func(type, *value_column);
      } else if (auto dict_column = dynamic_cast<DictionaryColumn<Type> *>(&column)) {
        func(type, *dict_column);
      } else if (auto ref_column = dynamic_cast<ReferenceColumn *>(&column)) {
        func(type, *ref_column);
      } else {
        Fail("Unrecognized column type encountered.");
      }

      return;
    }
  });
}

}  // namespace opossum
