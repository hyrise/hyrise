#pragma once

#include <boost/hana/ext/boost/mpl/vector.hpp>
#include <boost/hana/for_each.hpp>
#include <boost/hana/integral_constant.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace opossum {

namespace hana = boost::hana;

enum class JoinMode { Cross, Inner, Left_outer, Right_outer, Full_outer };

using ChunkID = uint32_t;
using ChunkOffset = uint32_t;
struct RowID {
  ChunkID chunk_id;
  ChunkOffset chunk_offset;
};
constexpr RowID NULL_ROW = RowID{0u, std::numeric_limits<ChunkOffset>::max()};
constexpr ChunkOffset NULL_VALUE = std::numeric_limits<ChunkOffset>::max();

using ValueID = uint32_t;  // Cannot be larger than ChunkOffset

using PosList = std::vector<RowID>;
using StringVector = std::vector<std::string>;

// This holds all possible data types. The left side of the pairs are the names, the right side are prototypes
// ("examples").
// These examples are later used with decltype() in the template magic below.
static auto column_types = hana::make_tuple(
    hana::make_pair("int", static_cast<int32_t>(123)), hana::make_pair("long", static_cast<int64_t>(123456789l)),
    hana::make_pair("float", 123.4f), hana::make_pair("double", 123.4), hana::make_pair("string", std::string("hi")));

// convert tuple of all types to sequence by first extracting the prototypes only and then applying decltype_
static auto types_as_hana_sequence = hana::transform(hana::transform(column_types, hana::second), hana::decltype_);
// convert hana sequence to mpl vector
using TypesAsMplVector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(types_as_hana_sequence));
// create boost::variant from mpl vector
using AllTypeVariant = typename boost::make_variant_over<TypesAsMplVector>::type;

// cast methods - from variant to specific type
template <typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type type_cast(AllTypeVariant value) {
  try {
    return boost::lexical_cast<T>(value);
  } catch (...) {
    return boost::numeric_cast<T>(boost::lexical_cast<double>(value));
  }
}

template <typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type type_cast(AllTypeVariant value) {
  // TODO(MD): is lexical_cast always necessary?
  return boost::lexical_cast<T>(value);
}

template <typename T>
typename std::enable_if<std::is_same<T, std::string>::value, T>::type type_cast(AllTypeVariant value) {
  return boost::lexical_cast<T>(value);
}

std::string to_string(const AllTypeVariant &x);

template <class base, template <typename...> class impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<base> make_unique_by_column_type(const std::string &type, ConstructorArgs &&... args) {
  std::unique_ptr<base> ret = nullptr;
  hana::for_each(column_types, [&](auto x) {
    if (std::string(hana::first(x)) == type) {
      typename std::remove_reference<decltype(hana::second(x))>::type prototype;
      ret = std::make_unique<impl<decltype(prototype), TemplateArgs...>>(std::forward<ConstructorArgs>(args)...);
      return;
    }
  });
  if (IS_DEBUG && !ret) throw std::runtime_error("unknown type " + type);
  return ret;
}

template <class base, template <typename...> class impl, class... TemplateArgs, class... ConstructorArgs>
std::shared_ptr<base> make_shared_by_column_type(const std::string &type, ConstructorArgs &&... args) {
  return std::move(
      make_unique_by_column_type<base, impl, TemplateArgs...>(type, std::forward<ConstructorArgs>(args)...));
}
}  // namespace opossum
