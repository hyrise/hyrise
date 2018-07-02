#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/equal.hpp>
#include <boost/hana/for_each.hpp>
#include <boost/hana/size.hpp>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "all_type_variant.hpp"
#include "storage/reference_column.hpp"
#include "storage/resolve_encoded_column_type.hpp"
#include "storage/value_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * Resolves a data type by creating an instance of a templated class and
 * returning it as a unique_ptr of its non-templated base class.
 *
 * @param data_type is an enum value of any of the supported data types
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
 *   auto impl = make_unique_by_data_type<BaseImpl, Impl>(DataType::String, var);
 *   impl->execute();
 */
template <class Base, template <typename...> class Impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<Base> make_unique_by_data_type(DataType data_type, ConstructorArgs&&... args) {
  DebugAssert(data_type != DataType::Null, "data_type cannot be null.");

  std::unique_ptr<Base> ret = nullptr;
  hana::for_each(data_type_pairs, [&](auto x) {
    if (hana::first(x) == data_type) {
      // The + before hana::second - which returns a reference - converts its return value
      // into a value so that we can access ::type
      using ColumnDataType = typename decltype(+hana::second(x))::type;
      ret = std::make_unique<Impl<ColumnDataType, TemplateArgs...>>(std::forward<ConstructorArgs>(args)...);
      return;
    }
  });
  return ret;
}

/**
 * Resolves two data types by creating an instance of a templated class and
 * returning it as a unique_ptr of its non-templated base class.
 * It does the same as make_unique_by_data_type but with two data types.
 *
 * @param data_type1 is an enum value of any of the supported column types
 * @param data_type2 is an enum value of any of the supported column types
 * @param args is a list of constructor arguments
 *
 * Note: We need to pass parameter packs explicitly for GCC due to the following bug:
 *       http://stackoverflow.com/questions/41769851/gcc-causes-segfault-for-lambda-captured-parameter-pack
 */
template <class Base, template <typename...> class Impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<Base> make_unique_by_data_types(DataType data_type1, DataType data_type2, ConstructorArgs&&... args) {
  DebugAssert(data_type1 != DataType::Null, "data_type1 cannot be null.");
  DebugAssert(data_type2 != DataType::Null, "data_type2 cannot be null.");

  std::unique_ptr<Base> ret = nullptr;
  hana::for_each(data_type_pairs, [&ret, &data_type1, &data_type2, &args...](auto x) {
    if (hana::first(x) == data_type1) {
      hana::for_each(data_type_pairs, [&ret, &data_type2, &args...](auto y) {
        if (hana::first(y) == data_type2) {
          using ColumnDataType1 = typename decltype(+hana::second(x))::type;
          using ColumnDataType2 = typename decltype(+hana::second(y))::type;
          ret = std::make_unique<Impl<ColumnDataType1, ColumnDataType2, TemplateArgs...>>(
              std::forward<ConstructorArgs>(args)...);
          return;
        }
      });
      return;
    }
  });
  return ret;
}

/**
 * Convenience function. Calls make_unique_by_data_type and casts the result into a shared_ptr.
 */
template <class Base, template <typename...> class impl, class... TemplateArgs, class... ConstructorArgs>
std::shared_ptr<Base> make_shared_by_data_type(DataType data_type, ConstructorArgs&&... args) {
  return make_unique_by_data_type<Base, impl, TemplateArgs...>(data_type, std::forward<ConstructorArgs>(args)...);
}

/**
 * Resolves a data type by passing a hana::type object on to a generic lambda
 *
 * @param data_type is an enum value of any of the supported column types
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
 *   resolve_data_type(data_type, [&](auto type) {
 *     using ColumnDataType = typename decltype(type)::type;
 *     const auto var = type_cast<ColumnDataType>(variant_from_elsewhere);
 *     process_variant(var);
 *
 *     process_type(type);
 *   });
 */
template <typename Functor>
void resolve_data_type(DataType data_type, const Functor& func) {
  DebugAssert(data_type != DataType::Null, "data_type cannot be null.");

  hana::for_each(data_type_pairs, [&](auto x) {
    if (hana::first(x) == data_type) {
      // The + before hana::second - which returns a reference - converts its return value into a value
      func(+hana::second(x));
      return;
    }
  });
}

/**
 * Given a BaseColumn and its known column type, resolve the column implementation and call the lambda
 *
 * @param func is a generic lambda or similar accepting a reference to a specialized column (value, dictionary,
 * reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   void process_column(ValueColumn<T>& column);
 *
 *   template <typename T>
 *   void process_column(DictionaryColumn<T>& column);
 *
 *   void process_column(ReferenceColumn& column);
 *
 *   resolve_column_type<T>(base_column, [&](auto& typed_column) {
 *     process_column(typed_column);
 *   });
 */
template <typename In, typename Out>
using ConstOutIfConstIn = std::conditional_t<std::is_const<In>::value, const Out, Out>;

template <typename ColumnDataType, typename BaseColumnType, typename Functor>
// BaseColumnType allows column to be const and non-const
std::enable_if_t<std::is_same<BaseColumn, std::remove_const_t<BaseColumnType>>::value>
/*void*/ resolve_column_type(BaseColumnType& column, const Functor& func) {
  using ValueColumnPtr = ConstOutIfConstIn<BaseColumnType, ValueColumn<ColumnDataType>>*;
  using ReferenceColumnPtr = ConstOutIfConstIn<BaseColumnType, ReferenceColumn>*;
  using EncodedColumnPtr = ConstOutIfConstIn<BaseColumnType, BaseEncodedColumn>*;

  if (auto value_column = dynamic_cast<ValueColumnPtr>(&column)) {
    func(*value_column);
  } else if (auto ref_column = dynamic_cast<ReferenceColumnPtr>(&column)) {
    func(*ref_column);
  } else if (auto encoded_column = dynamic_cast<EncodedColumnPtr>(&column)) {
    resolve_encoded_column_type<ColumnDataType>(*encoded_column, func);
  } else {
    Fail("Unrecognized column type encountered.");
  }
}

/**
 * Resolves a data type by passing a hana::type object and the downcasted column on to a generic lambda
 *
 * @param data_type is an enum value of any of the supported column types
 * @param func is a generic lambda or similar accepting two parameters: a hana::type object and
 *   a reference to a specialized column (value, dictionary, reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   void process_column(hana::basic_type<T> type, ValueColumn<T>& column);
 *
 *   template <typename T>
 *   void process_column(hana::basic_type<T> type, DictionaryColumn<T>& column);
 *
 *   template <typename T>
 *   void process_column(hana::basic_type<T> type, ReferenceColumn& column);
 *
 *   resolve_data_and_column_type(base_column, [&](auto type, auto& typed_column) {
 *     process_column(type, typed_column);
 *   });
 */
template <typename Functor, typename BaseColumnType>  // BaseColumnType allows column to be const and non-const
std::enable_if_t<std::is_same<BaseColumn, std::remove_const_t<BaseColumnType>>::value>
/*void*/ resolve_data_and_column_type(BaseColumnType& column, const Functor& func) {
  resolve_data_type(column.data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    resolve_column_type<ColumnDataType>(column, [&](auto& typed_column) { func(type, typed_column); });
  });
}

/**
 * This function returns the DataType of a data type based on the definition in data_type_pairs.
 */
template <typename T>
constexpr DataType data_type_from_type() {
  static_assert(hana::contains(data_types, hana::type_c<T>), "Type not a valid column type.");

  return hana::fold_left(data_type_pairs, DataType{}, [](auto data_type, auto type_tuple) {
    // check whether T is one of the column types
    if (hana::type_c<T> == hana::second(type_tuple)) {
      return hana::first(type_tuple);
    }

    return data_type;
  });
}

/**
 * This function returns the DataType of an AllTypeVariant
 *
 * Note: DataType and AllTypeVariant are defined in a way such that
 *       the indices in DataType and AllTypeVariant match.
 */
inline DataType data_type_from_all_type_variant(const AllTypeVariant& all_type_variant) {
  return static_cast<DataType>(all_type_variant.which());
}

}  // namespace opossum
