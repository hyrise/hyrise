#pragma once

#include <boost/hana/equal.hpp>
#include <boost/hana/for_each.hpp>
#include <boost/hana/size.hpp>

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "all_type_variant.hpp"
#include "utils/assert.hpp"

#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * Resolves a type string by creating an instance of a templated class and
 * returning it as a unique_ptr of its non-templated base class.
 *
 * @param type is a string representation of any of the supported column types
 * @param args is a list of constructor arguments
 *
 *
 * Example:
 *
 *   class BaseImpl {
 *    public:
 *     virtual void execute() = 0;
 *   };
 *
 *   template <typename T>
 *   class Impl : public BaseImpl {
 *    public:
 *     Impl(int var) : _var{var} { ... }
 *
 *     void execute() override { ... }
 *   };
 *
 *   constexpr auto var = 12;
 *   auto impl = make_unique_by_column_type<BaseImpl, Impl>("string", var);
 *   impl->execute();
 */
template <class Base, template <typename...> class Impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<Base> make_unique_by_column_type(const std::string &type, ConstructorArgs &&... args) {
  std::unique_ptr<Base> ret = nullptr;
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      // The + before hana::second - which returns a reference - converts its return value
      // into a value so that we can access ::type
      using ColumnType = typename decltype(+hana::second(x))::type;
      ret = std::make_unique<Impl<ColumnType, TemplateArgs...>>(std::forward<ConstructorArgs>(args)...);
      return;
    }
  });
  DebugAssert(static_cast<bool>(ret), "unknown type " + type);
  return ret;
}

/**
 * Resolves two type strings by creating an instance of a templated class and
 * returning it as a unique_ptr of its non-templated base class.
 * It does the same as make_unique_by_column_type but with two type strings.
 *
 * @param type1 is a string representation of any of the supported column types
 * @param type2 is a string representation of any of the supported column types
 * @param args is a list of constructor arguments
 *
 * Note: We need to pass parameter packs explicitly for GCC due to the following bug:
 *       http://stackoverflow.com/questions/41769851/gcc-causes-segfault-for-lambda-captured-parameter-pack
 */
template <class Base, template <typename...> class Impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<Base> make_unique_by_column_types(const std::string &type1, const std::string &type2,
                                                  ConstructorArgs &&... args) {
  std::unique_ptr<Base> ret = nullptr;
  hana::for_each(column_types, [&ret, &type1, &type2, &args...](auto x) {
    if (std::string(hana::first(x)) == type1) {
      hana::for_each(column_types, [&ret, &type2, &args...](auto y) {
        if (std::string(hana::first(y)) == type2) {
          using ColumnType1 = typename decltype(+hana::second(x))::type;
          using ColumnType2 = typename decltype(+hana::second(y))::type;
          ret = std::make_unique<Impl<ColumnType1, ColumnType2, TemplateArgs...>>(
            std::forward<ConstructorArgs>(args)...);
          return;
        }
      });
      return;
    }
  });
  DebugAssert(static_cast<bool>(ret), "unknown type " + type1 + " or " + type2);
  return ret;
}

/**
 * Convenience function. Calls make_unique_by_column_type and casts the result into a shared_ptr.
 */
template <class Base, template <typename...> class impl, class... TemplateArgs, class... ConstructorArgs>
std::shared_ptr<Base> make_shared_by_column_type(const std::string &type, ConstructorArgs &&... args) {
  return make_unique_by_column_type<Base, impl, TemplateArgs...>(type, std::forward<ConstructorArgs>(args)...);
}

/**
 * Resolves a type string by passing a hana::type object on to a generic lambda
 *
 * @param type_string is a string representation of any of the supported column types
 * @param func is a generic lambda or similar accepting a hana::type object
 *
 *
 * Note on hana::type (taken from Boost.Hana documentation):
 *
 * For subtle reasons having to do with ADL, the actual representation of hana::type is
 * implementation-defined. In particular, hana::type may be a dependent type, so one
 * should not attempt to do pattern matching on it. However, one can assume that hana::type
 * inherits from hana::basic_type, which can be useful when declaring overloaded functions.
 *
 * This means that we need to use hana::basic_type as a parameter in methods so that the
 * underlying type can be deduced from the object.
 *
 *
 * Note on generic lambdas (taken from paragraph 5.1.2/5 of the C++14 Standard Draft n3690):
 *
 * For a generic lambda, the closure type has a public inline function call operator member template (14.5.2)
 * whose template-parameter-list consists of one invented type template-parameter for each occurrence of auto
 * in the lambda’s parameter-declaration-clause, in order of appearance. Example:
 *
 *   auto lambda = [] (auto a) { return a; };
 *
 *   class // unnamed {
 *    public:
 *     template<typename T>
 *     auto operator()(T a) const { return a; }
 *   };
 *
 *
 * Example:
 *
 *   template <typename T>
 *   process_variant(const T& var);
 *
 *   template <typename T>
 *   process_type(hana::basic_type<T> type);  // note: parameter type needs to be hana::basic_type not hana::type!
 *
 *   resolve_data_type(type_string, [&](auto type) {
 *     using Type = typename decltype(type)::type;
 *     const auto var = type_cast<Type>(variant_from_elsewhere);
 *     process_variant(var);
 *
 *     process_type(type);
 *   });
 */
template <typename Functor>
void resolve_data_type(const std::string &type_string, const Functor &func) {
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type_string) {
      // The + before hana::second - which returns a reference - converts its return value into a value
      func(+hana::second(x));
      return;
    }
  });
}

/**
 * Given a BaseColumn and its known DataType, resolve the column implementation and call the lambda
 *
 * @param func is a generic lambda or similar accepting a reference to a specialized column (value, dictionary,
 * reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   process_column(ValueColumn<T>& column);
 *
 *   template <typename T>
 *   process_column(DictionaryColumn<T>& column);
 *
 *   process_column(ReferenceColumn& column);
 *
 *   resolve_column_type<T>(base_column, [&](auto& typed_column) {
 *     process_column(typed_column);
 *   });
 */
template <typename DataType, typename Functor>
void resolve_column_type(BaseColumn &column, const Functor &func) {
  if (auto value_column = dynamic_cast<ValueColumn<DataType> *>(&column)) {
    func(*value_column);
  } else if (auto dict_column = dynamic_cast<DictionaryColumn<DataType> *>(&column)) {
    func(*dict_column);
  } else if (auto ref_column = dynamic_cast<ReferenceColumn *>(&column)) {
    func(*ref_column);
  } else {
    Fail("Unrecognized column type encountered.");
  }
}

/**
 * Resolves a type string by passing a hana::type object and the downcasted column on to a generic lambda
 *
 * @param type_string is a string representation of any of the supported column types
 * @param func is a generic lambda or similar accepting two parameters: a hana::type object and
 *   a reference to a specialized column (value, dictionary, reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   process_column(hana::basic_type<T> type, ValueColumn<T>& column);
 *
 *   template <typename T>
 *   process_column(hana::basic_type<T> type, DictionaryColumn<T>& column);
 *
 *   template <typename T>
 *   process_column(hana::basic_type<T> type, ReferenceColumn& column);
 *
 *   resolve_data_and_column_type(column_type, base_column, [&](auto type, auto& typed_column) {
 *     process_column(type, typed_column);
 *   });
 */
template <typename Functor>
void resolve_data_and_column_type(const std::string &type_string, BaseColumn &column, const Functor &func) {
  resolve_data_type(type_string, [&](auto data_type) {
    using DataType = typename decltype(data_type)::type;

    resolve_column_type<DataType>(column, [&](auto &typed_column) { func(data_type, typed_column); });
  });
}

/**
 * This function returns the name of an Opossum datatype based on the definition in column_types.
 */
template <typename T>
std::string type_string_from_type() {
  const auto func = [](std::string s, auto type_tuple) {
    // a matching type was found before
    if (s.size() != 0) {
      return s;
    }

    // check whether T is one of the Opossum datatypes
    if (hana::type_c<T> == hana::second(type_tuple)) {
      return std::string{hana::first(type_tuple)};
    }

    return std::string{};
  };

  const auto type_string = hana::fold_left(column_types, std::string{}, func);
  Assert(type_string.size() > 0, "Trying to parse unknown type which is not part of AllTypeVariant");

  return type_string;
}

/**
 * This function returns the name of an Opossum datatype based on the definition in column_types.
 */
inline std::string type_string_from_all_type_variant(const AllTypeVariant &all_type_variant) {
  // Special case for NullValue data type
  if (all_type_variant.which() == 0) {
    return "None";
  }

  // iterate over column_types
  int column_types_index = 1;

  auto func = [all_type_variant, &column_types_index](std::string s, auto type_tuple) {
    // a matching type was found before
    if (s.size() != 0) {
      return s;
    }

    if (all_type_variant.which() == column_types_index) {
      column_types_index++;
      return std::string{hana::first(type_tuple)};
    }

    column_types_index++;
    return std::string{};
  };

  return hana::fold_left(column_types, std::string{}, func);
}

}  // namespace opossum
