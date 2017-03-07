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

using ChunkID = uint32_t;
using ChunkOffset = uint32_t;
struct RowID {
  ChunkID chunk_id;
  ChunkOffset chunk_offset;

  bool operator<(const RowID &rhs) const {
    return std::tie(chunk_id, chunk_offset) < std::tie(rhs.chunk_id, rhs.chunk_offset);
  }
};
constexpr ChunkOffset INVALID_CHUNK_OFFSET = std::numeric_limits<ChunkOffset>::max();

using ColumnID = uint16_t;
using ValueID = uint32_t;  // Cannot be larger than ChunkOffset
using WorkerID = uint32_t;
using NodeID = uint32_t;
using TaskID = uint32_t;
using CpuID = uint32_t;

using CommitID = uint32_t;
using TransactionID = uint32_t;

using StringLength = uint16_t;     // The length of column value strings must fit in this type.
using ColumnNameLength = uint8_t;  // The length of column names must fit in this type.
using AttributeVectorWidth = uint8_t;

using PosList = std::vector<RowID>;

class ColumnName {
 public:
  explicit ColumnName(const std::string &name) : _name(name) {}

  operator std::string() const { return _name; }

 protected:
  const std::string _name;
};

constexpr NodeID INVALID_NODE_ID = std::numeric_limits<NodeID>::max();
constexpr TaskID INVALID_TASK_ID = std::numeric_limits<TaskID>::max();
constexpr CpuID INVALID_CPU_ID = std::numeric_limits<CpuID>::max();
constexpr WorkerID INVALID_WORKER_ID = std::numeric_limits<WorkerID>::max();

constexpr NodeID CURRENT_NODE_ID = std::numeric_limits<NodeID>::max() - 1;

// The Scheduler currently supports just these 2 priorities, subject to change.
enum class SchedulePriority {
  Normal = 1,  // Schedule task at the end of the queue
  High = 0     // Schedule task at the beginning of the queue
};

// This holds all possible data types. The left side of the pairs are the names, the right side are prototypes
// ("examples").
// These examples are later used with decltype() in the template magic below.
static auto column_types = hana::make_tuple(
    hana::make_pair("int", static_cast<int32_t>(123)), hana::make_pair("long", static_cast<int64_t>(123456789l)),
    hana::make_pair("float", 123.4f), hana::make_pair("double", 123.4), hana::make_pair("string", std::string("hi")));

// convert tuple of all types to sequence by first extracting the prototypes
// only and then applying decltype_
static auto types_as_hana_sequence = hana::transform(hana::transform(column_types, hana::second), hana::decltype_);
// convert hana sequence to mpl vector
using TypesAsMplVector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(types_as_hana_sequence));
// create boost::variant from mpl vector
using AllTypeVariant = typename boost::make_variant_over<TypesAsMplVector>::type;

/**
 * AllParameterVariant holds either an AllTypeVariant or a ColumnName.
 * It should be used to generalize Opossum operator calls.
 */
static auto parameter_types = hana::make_tuple(hana::make_pair("AllTypeVariant", AllTypeVariant(123)),
                                               hana::make_pair("ColumnName", ColumnName("column_name")));

// convert tuple of all types to sequence by first extracting the prototypes only and then applying decltype_
static auto parameter_types_as_hana_sequence =
    hana::transform(hana::transform(parameter_types, hana::second), hana::decltype_);
// convert hana sequence to mpl vector
using ParameterTypesAsMplVector =
    decltype(hana::to<hana::ext::boost::mpl::vector_tag>(parameter_types_as_hana_sequence));
// create boost::variant from mpl vector
using AllParameterVariant = typename boost::make_variant_over<ParameterTypesAsMplVector>::type;

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

/*
We need to pass parameter packs explicitly for GCC due to the following bug:
http://stackoverflow.com/questions/41769851/gcc-causes-segfault-for-lambda-captured-parameter-pack
*/
template <class base, template <typename...> class impl, class... TemplateArgs, typename... ConstructorArgs>
std::unique_ptr<base> make_unique_by_column_types(const std::string &type1, const std::string &type2,
                                                  ConstructorArgs &&... args) {
  std::unique_ptr<base> ret = nullptr;
  hana::for_each(column_types, [&ret, &type1, &type2, &args...](auto x) {
    if (std::string(hana::first(x)) == type1) {
      hana::for_each(column_types, [&ret, &type1, &type2, &args...](auto y) {
        if (std::string(hana::first(y)) == type2) {
          typename std::remove_reference<decltype(hana::second(x))>::type prototype1;
          typename std::remove_reference<decltype(hana::second(y))>::type prototype2;
          ret = std::make_unique<impl<decltype(prototype1), decltype(prototype2), TemplateArgs...>>(
              std::forward<ConstructorArgs>(args)...);
          return;
        }
      });
      return;
    }
  });
  if (IS_DEBUG && !ret) throw std::runtime_error("unknown type " + type1 + " or " + type2);
  return ret;
}

template <class base, template <typename...> class impl, class... TemplateArgs, class... ConstructorArgs>
std::shared_ptr<base> make_shared_by_column_type(const std::string &type, ConstructorArgs &&... args) {
  return make_unique_by_column_type<base, impl, TemplateArgs...>(type, std::forward<ConstructorArgs>(args)...);
}
}  // namespace opossum
