#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <boost/hana/contains.hpp>
#include <boost/hana/equal.hpp>
#include <boost/hana/for_each.hpp>
#include <boost/hana/size.hpp>

#include "all_type_variant.hpp"
#include "storage/reference_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/value_segment.hpp"
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
 *     const auto var = boost::get<ColumnDataType>(variant_from_elsewhere);
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
 * Given a BaseSegment and its known column type, resolve the segment implementation and call the lambda
 *
 * @param func is a generic lambda or similar accepting a reference to a specialized segment (value, dictionary,
 * reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   void process_segment(ValueSegment<T>& segment);
 *
 *   template <typename T>
 *   void process_segment(DictionarySegment<T>& segment);
 *
 *   void process_segment(ReferenceSegment& segment);
 *
 *   resolve_segment_type<T>(base_segment, [&](auto& typed_segment) {
 *     process_segment(typed_segment);
 *   });
 */
template <typename In, typename Out>
using ConstOutIfConstIn = std::conditional_t<std::is_const_v<In>, const Out, Out>;

template <typename ColumnDataType, typename BaseSegmentType, typename Functor>
// BaseSegmentType allows segment to be const and non-const
std::enable_if_t<std::is_same_v<BaseSegment, std::remove_const_t<BaseSegmentType>>>
/*void*/ resolve_segment_type(BaseSegmentType& segment, const Functor& func) {
  using ValueSegmentPtr = ConstOutIfConstIn<BaseSegmentType, ValueSegment<ColumnDataType>>*;
  using ReferenceSegmentPtr = ConstOutIfConstIn<BaseSegmentType, ReferenceSegment>*;
  using EncodedSegmentPtr = ConstOutIfConstIn<BaseSegmentType, BaseEncodedSegment>*;

  if (auto value_segment = dynamic_cast<ValueSegmentPtr>(&segment)) {
    func(*value_segment);
  } else if (auto ref_segment = dynamic_cast<ReferenceSegmentPtr>(&segment)) {
    func(*ref_segment);
  } else if (auto encoded_segment = dynamic_cast<EncodedSegmentPtr>(&segment)) {
    resolve_encoded_segment_type<ColumnDataType>(*encoded_segment, func);
  } else {
    Fail("Unrecognized column type encountered.");
  }
}

/**
 * Resolves a data type by passing a hana::type object and the downcasted segment on to a generic lambda
 *
 * @param data_type is an enum value of any of the supported column types
 * @param func is a generic lambda or similar accepting two parameters: a hana::type object and
 *   a reference to a specialized segment (value, dictionary, reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   void process_segment(hana::basic_type<T> type, ValueSegment<T>& segment);
 *
 *   template <typename T>
 *   void process_segment(hana::basic_type<T> type, DictionarySegment<T>& segment);
 *
 *   template <typename T>
 *   void process_segment(hana::basic_type<T> type, ReferenceSegment& segment);
 *
 *   resolve_data_and_segment_type(base_segment, [&](auto type, auto& typed_segment) {
 *     process_segment(type, typed_segment);
 *   });
 */
template <typename Functor, typename BaseSegmentType>  // BaseSegmentType allows segment to be const and non-const
std::enable_if_t<std::is_same_v<BaseSegment, std::remove_const_t<BaseSegmentType>>>
/*void*/ resolve_data_and_segment_type(BaseSegmentType& segment, const Functor& func) {
  resolve_data_type(segment.data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    resolve_segment_type<ColumnDataType>(segment, [&](auto& typed_segment) { func(type, typed_segment); });
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

}  // namespace opossum
