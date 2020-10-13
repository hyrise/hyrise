#pragma once

#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <variant>

#include <aws/core/utils/json/JsonSerializer.h>
#include <magic_enum.hpp>

// TODO(CAJan93): #include "../assert.hpp"
// TODO(CAJan93): #include "../string.hpp"
// TODO(CAJan93): #include "assert.hpp"
// TODO(CAJan93): #include "../types/types.hpp"
#include "../../expression/abstract_expression.hpp"
#include "../../expression/abstract_predicate_expression.hpp"
#include "../../expression/aggregate_expression.hpp"
#include "../../expression/arithmetic_expression.hpp"
#include "../../expression/between_expression.hpp"
#include "../../expression/binary_predicate_expression.hpp"
#include "../../expression/case_expression.hpp"
#include "../../expression/cast_expression.hpp"
#include "../../expression/exists_expression.hpp"
#include "../../expression/extract_expression.hpp"
#include "../../expression/in_expression.hpp"
#include "../../expression/is_null_expression.hpp"
#include "../../expression/list_expression.hpp"
#include "../../expression/logical_expression.hpp"
#include "../../expression/placeholder_expression.hpp"
#include "../../expression/pqp_column_expression.hpp"
#include "../../expression/pqp_subquery_expression.hpp"
#include "../../expression/unary_minus_expression.hpp"
#include "../../expression/value_expression.hpp"
#include "../../logical_query_plan/predicate_node.hpp"
#include "../../operators/abstract_aggregate_operator.hpp"
#include "../../operators/abstract_operator.hpp"
#include "../../operators/aggregate_hash.hpp"
#include "../../operators/aggregate_sort.hpp"
#include "../../operators/alias_operator.hpp"
#include "../../operators/get_table.hpp"
#include "../../operators/limit.hpp"
#include "../../operators/projection.hpp"
#include "../../operators/table_scan.hpp"
#include "../../operators/validate.hpp"
#include "../../types.hpp"
#include "../../utils/assert.hpp"
#include "../types/get_inner_type.hpp"
#include "../types/is_integral.hpp"
#include "../types/is_smart_ptr.hpp"
#include "../types/is_vector.hpp"
#include "has_member.hpp"
#include "string.hpp"

namespace opossum {

// forward declaration and aliases
class AbstractExpression;
class AbstractAggregateOperator;
class AggregateExpression;
class AggregateHash;
class AggregateSort;
class AliasOperator;
class ArithmeticExpression;
class BetweenExpression;
class BinaryPredicateExpression;
class GetTable;
class InExpression;
class IsNullExpression;
class PQPColumnExpression;
class PredicateNode;
class TableScan;
class Validate;
class ValueExpression;
using jsonVal = Aws::Utils::Json::JsonValue;
using jsonView = Aws::Utils::Json::JsonView;

// TODO(CAJan93): Remove this function. This is from assert.hpp
template <bool b>
struct StaticAssert {};

// TODO(CAJan93): Remove this function. This is from assert.hpp
// template specialized on true
template <>
struct StaticAssert<true> {
  static void stat_assert(const std::string& msg) { (void)msg; }
};

// TODO(CAJan93): Remove this function. This is from helper.hpp
/**
 * Joins the provided arguments into a string using a stringstream.
 * @param args The values to join into a string
 * @return Returns a string
 */
inline constexpr auto JOIN_TO_STR = [](auto... args) -> std::string {
  std::stringstream strs;
  strs << std::boolalpha;
  (strs << ... << args);
  return strs.str();
};

class JsonSerializer {
  /***
  * convert a vector<T> into a json object
  * {
  *  "0": 1
  *  "1": {
  *      // some nested value
  *    }
  * }
  */
  template <typename T>
  static jsonVal vec_to_json(const std::vector<T>& vec);

  // sequence for
  template <typename T, T... S, typename F>
  static constexpr void for_sequence(std::integer_sequence<T, S...>, F&& f);

  // retrieve data from json
  template <typename T>
  static T as_any(const jsonView&, const std::string&);

  // set data in json
  template <typename T>
  static void with_any(jsonVal& data, const std::string& key, const T& val);

  using string_t =
      std::__cxx11::basic_string<char, std::char_traits<char>, boost::container::pmr::polymorphic_allocator<char>>;

 public:
  // unserialize function
  template <typename T>
  static T from_json(const jsonView& data);

  template <typename T>
  static auto from_json_str(const std::string& json_str);

  template <typename T>
  static jsonVal to_json(const T& object);

  template <typename T>
  static std::string to_json_str(const T& object);
};

// sequence for
template <typename T, T... S, typename F>
constexpr void JsonSerializer::for_sequence(std::integer_sequence<T, S...>, F&& f) {
  using unpack_t = int[];
  (void)unpack_t{(static_cast<void>(f(std::integral_constant<T, S>{})), 0)..., 0};
}

template <>
inline int JsonSerializer::as_any<int>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key),
              JOIN_TO_STR("key ", key, " does not exist           ", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsIntegerType(),
              JOIN_TO_STR("key ", key, " is not an integer type\n           ", "JSON: ", value.WriteReadable()));
  return value.GetInteger(key);
}

template <>
inline ChunkID JsonSerializer::as_any<ChunkID>(const jsonView& value, const std::string& key) {
  return static_cast<ChunkID>(as_any<int>(value, key));
}

template <>
inline ChunkOffset JsonSerializer::as_any<ChunkOffset>(const jsonView& value, const std::string& key) {
  return static_cast<ChunkOffset>(as_any<int>(value, key));
}

template <>
inline ColumnID JsonSerializer::as_any<ColumnID>(const jsonView& value, const std::string& key) {
  return static_cast<ColumnID>(as_any<int>(value, key));
}

template <>
inline CpuID JsonSerializer::as_any<CpuID>(const jsonView& value, const std::string& key) {
  return static_cast<CpuID>(as_any<int>(value, key));
}

template <>
inline NodeID JsonSerializer::as_any<NodeID>(const jsonView& value, const std::string& key) {
  return static_cast<NodeID>(as_any<int>(value, key));
}

template <>
inline ValueID JsonSerializer::as_any<ValueID>(const jsonView& value, const std::string& key) {
  return static_cast<ValueID>(as_any<int>(value, key));
}

template <>
inline bool JsonSerializer::as_any<bool>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key),
              JOIN_TO_STR("key ", key, " does not exist\n           ", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsBool(),
              JOIN_TO_STR("key ", key, " is not a boolean\n           ", "JSON: ", value.WriteReadable()));
  return value.GetBool(key);
}

template <>
inline std::string JsonSerializer::as_any<std::string>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key),
              JOIN_TO_STR("key ", key, " does not exist\n           ", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsString(),
              JOIN_TO_STR("key ", key, " is not of type string\n           ", "JSON: ", value.WriteReadable()));
  return value.GetString(key);
}

template <>
inline double JsonSerializer::as_any<double>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key),
              JOIN_TO_STR("key ", key, " does not exist\n           ", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsFloatingPointType(),
              JOIN_TO_STR("key ", key, " is not a floting point type\n           ", "JSON: ", value.WriteReadable()));
  return value.GetDouble(key);
}

// TODO(CAJan93): remove below case
template <>
inline std::variant<int, double, std::string> JsonSerializer::as_any<std::variant<int, double, std::string>>(
    const jsonView& value, const std::string& key) {
  if (value.GetObject(key).IsIntegerType()) return value.GetInteger(key);
  if (value.GetObject(key).IsFloatingPointType()) return value.GetDouble(key);
  if (value.GetObject(key).IsString()) return value.GetString(key);
  Fail("json deserializer only support variants with <int, double, string>");
  return -1;
}

/**
 * AllTypeVariant is encoded like this:
 * "value": {
 *      "val_t":  5
 *      "val":    "some string"
 * }
 * Retrieve an AllTypeVariant by passing the key "value". This function will then use the key
 * "val_t" to determine the type of the value. The key "val" will be used to retrieve the value
 */
template <>
inline AllTypeVariant JsonSerializer::as_any<AllTypeVariant>(const jsonView& value, const std::string& key) {
  // TODO(CAJan93): Check if key val_t and key exist
  Assert(value.GetObject(key).KeyExists("val_t"), JOIN_TO_STR("val_t does not exist in json ", value.WriteReadable()));
  Assert(value.GetObject(key).GetObject("val_t").IsIntegerType(),
         JOIN_TO_STR("val_t is not of integer type ", value.WriteReadable()));
  const int value_t = value.GetObject(key).GetInteger("val_t");
  switch (value_t) {
    case 0:
      return nullptr;
    case 1:
    case 2:
      // use one convenience function for this
      Assert(value.GetObject(key).KeyExists("val"), JOIN_TO_STR("val does not exist in json ", value.WriteReadable()));
      Assert(value.GetObject(key).GetObject("val").IsIntegerType(),
             JOIN_TO_STR("val is not of integer type ", value.WriteReadable()));
      return value.GetObject(key).GetInteger("val");
    case 3:
    case 4:
      Assert(value.GetObject(key).KeyExists("val"), JOIN_TO_STR("val does not exist in json ", value.WriteReadable()));
      Assert(value.GetObject(key).GetObject("val").IsFloatingPointType(),
             JOIN_TO_STR("val is not of integer type ", value.WriteReadable()));
      return value.GetObject(key).GetDouble("val");
    case 5: {
      Assert(value.GetObject(key).KeyExists("val"), JOIN_TO_STR("val does not exist in json ", value.WriteReadable()));
      Assert(value.GetObject(key).GetObject("val").IsString(),
             JOIN_TO_STR("val is not of integer type ", value.WriteReadable()));
      const std::string s = value.GetObject(key).GetString("val");
      return string_t(s);
    }

    default:
      Fail(JOIN_TO_STR("Unable to retrieve value for AllTypeVariant. Json was ", value.GetObject(key).WriteReadable()));
  }
  return -1;
}

// for non-trivial objects
template <typename T>
inline T JsonSerializer::as_any(const jsonView& value, const std::string& key) {
  typedef typename std::remove_cv_t<T> without_cv_t;
  typedef typename std::remove_pointer_t<without_cv_t> without_cv_value_t;

  // e.g. as_any<std::string>(v, k) == as_any<const std::string>(v, k)
  if constexpr (!std::is_same<without_cv_t, T>::value) return as_any<without_cv_t>(value, key);

  if constexpr (std::is_pointer<without_cv_t>::value) {
    // nullpointers
    if (value.GetObject(key).IsString() && value.GetString(key) == "NULL") {
      return nullptr;
    }
    if (value.GetObject(key).IsObject()) {
      if constexpr (has_member_properties<without_cv_value_t>::value ||
                    std::is_same<without_cv_value_t, AbstractOperator>::value ||
                    std::is_same<without_cv_value_t, AbstractExpression>::value) {
        // handle nested object (pointer)
        jsonView sub_json = value.GetObject(key);
        // T:: properties exist
        without_cv_t sub_obj = from_json<without_cv_t>(sub_json);
        return sub_obj;
      }
      Fail(JOIN_TO_STR("Unable to process key ", key, "\n           Key is a raw pointer with the type ",
                       typeid(T).name(), "*. Current JSON object is\n", value.WriteReadable()));
    } else {
      /*
      TODO(CAJan93): Hotfix. I think I get an issue here, because the
      compiler does not know that this path is never chosen at runtime with
      without_cv_value_t == AbstractOperator
      */
      if constexpr (!std::is_same<without_cv_value_t, AbstractOperator>::value &&
                    !std::is_same<without_cv_value_t, AbstractExpression>::value) {
        without_cv_value_t sub_obj = as_any<without_cv_value_t>(value, key);
        without_cv_value_t* new_sub_obj = new without_cv_value_t{sub_obj};
        return new_sub_obj;
      } else {
        const std::string type =
            std::is_same<without_cv_value_t, AbstractOperator>::value ? "AbstractOperator" : "AbstractExpression";
        Fail(JOIN_TO_STR("Unable to serialize abstract type ", type));
      }
    }
  } else if constexpr (is_weak_ptr<T>::value) {
    Fail("weak ptr currently not supported");
    return -1;
  } else if constexpr (is_smart_ptr<without_cv_t>::value) {
    StaticAssert<!is_unique_ptr<without_cv_t>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");

    typedef typename std::remove_cv_t<without_cv_t> smart_ptr_t;
    typedef get_inner_t<smart_ptr_t> inner_t;            // type of the object the pointer is pointing to
    inner_t* object_ptr = as_any<inner_t*>(value, key);  // a pointer to such an object

    return smart_ptr_t(object_ptr);
  } else {
    if constexpr (std::is_enum<without_cv_t>::value) {
      Assert(value.KeyExists(key),
             JOIN_TO_STR("Unable to extract key '", key, "' from Json. Json is ", value.WriteReadable()));
      Assert(value.GetObject(key).IsString(),
             JOIN_TO_STR("Value at key '", key,
                         "' is representing an enum. Value has to be of type string, but is not. Value is ",
                         value.GetObject(key).WriteReadable(), "Json is ", value.WriteReadable()));
      auto enum_opt = magic_enum::enum_cast<without_cv_t>(value.GetString(key));
      if (!enum_opt.has_value()) {
        Fail(JOIN_TO_STR("Unable to create enum of type ", typeid(without_cv_t).name(), "from string '",
                         value.GetString(key), "'"));
      }
      return enum_opt.value();
    } else {
      if (value.GetObject(key).IsObject()) {
        if constexpr (has_member_properties<without_cv_t>::value) {
          // handle nested object (pointer or non-pointer)
          jsonView sub_json = value.GetObject(key);
          without_cv_t sub_obj = from_json<without_cv_t>(sub_json);
          without_cv_t new_sub_obj{sub_obj};
          return new_sub_obj;
        } else if constexpr (is_vector<without_cv_t>::value) {
          // deserialize a vector
          const jsonView obj = value.GetObject(key);
          without_cv_t vec;
          typedef get_inner_vec_t<without_cv_t> vec_inner_t;
          typedef std::remove_cv_t<vec_inner_t> without_cv_vec_inner_t;

          if constexpr (std::is_pointer<without_cv_vec_inner_t>::value) {
            typedef std::remove_pointer_t<without_cv_vec_inner_t> without_ptr_without_cv_vec_inner_t;
            for (size_t idx = 0; obj.KeyExists(std::to_string(idx)); ++idx) {
              const jsonView data = obj.GetObject(std::to_string(idx));
              if constexpr (has_member_properties<without_ptr_without_cv_vec_inner_t>::value) {
                without_ptr_without_cv_vec_inner_t* tmp = from_json<without_cv_vec_inner_t>(data);
                vec.emplace_back(tmp);  // serializer only supports vectors and no other containers
              } else if constexpr (std::is_same<without_ptr_without_cv_vec_inner_t, int>::value) {
                without_ptr_without_cv_vec_inner_t* tmp = new without_ptr_without_cv_vec_inner_t(data.AsInteger());
                vec.emplace_back(tmp);
              } else if constexpr (std::is_same<without_ptr_without_cv_vec_inner_t, double>::value) {
                without_ptr_without_cv_vec_inner_t* tmp = new without_ptr_without_cv_vec_inner_t(data.AsDouble());
                vec.emplace_back(tmp);
              } else if constexpr (std::is_same<without_ptr_without_cv_vec_inner_t, std::string>::value) {
                without_ptr_without_cv_vec_inner_t* tmp = new without_ptr_without_cv_vec_inner_t(data.AsString());
                vec.emplace_back(tmp);
              } else if constexpr (std::is_same<without_ptr_without_cv_vec_inner_t, bool>::value) {
                without_ptr_without_cv_vec_inner_t* tmp = new without_ptr_without_cv_vec_inner_t(data.AsBool());
                vec.emplace_back(tmp);
              } else {
                Fail("Unsupported vector type");
              }
            }
          } else if constexpr (is_smart_ptr<without_cv_vec_inner_t>::value) {
            StaticAssert<!is_unique_ptr<without_cv_t>::value>::stat_assert(
                "Unique pointers are currently not supported by this json serializer");
            StaticAssert<!is_weak_ptr<without_cv_t>::value>::stat_assert(
                "Weak pointers are currently not supported by this json serializer");
            typedef without_cv_vec_inner_t smart_ptr_t;  // type of the smart pointer
            typedef get_inner_t<without_cv_vec_inner_t>
                smart_ptr_inner_t;  // type of the object the pointer is pointing to
            for (size_t idx = 0; obj.KeyExists(std::to_string(idx)); ++idx) {
              const jsonView data = obj.GetObject(std::to_string(idx));
              //  if constexpr (has_member_properties<without_cv_vec_inner_t>::value) {
              smart_ptr_inner_t* t = from_json<smart_ptr_inner_t*>(data);
              smart_ptr_t ptr(t);
              vec.emplace_back(ptr);
            }
          } else {
            for (size_t idx = 0; obj.KeyExists(std::to_string(idx)); ++idx) {
              const jsonView data = obj.GetObject(std::to_string(idx));
              if constexpr (has_member_properties<without_cv_vec_inner_t>::value) {
                vec.emplace_back(from_json<without_cv_vec_inner_t>(
                    data));  // serializer only supports vectors and no other containers
              } else if constexpr (is_integral<without_cv_vec_inner_t>::value) {
                vec.emplace_back(data.AsInteger());
              } else if constexpr (std::is_same<without_cv_vec_inner_t, double>::value) {
                vec.emplace_back(data.AsDouble());
              } else if constexpr (std::is_same<without_cv_vec_inner_t, std::string>::value) {
                vec.emplace_back(data.AsString());
              } else if constexpr (std::is_same<without_cv_vec_inner_t, bool>::value) {
                vec.emplace_back(data.AsBool());
              } else {
                Fail(JOIN_TO_STR("Unsupported vector type '", typeid(without_cv_vec_inner_t).name(), "'. Json is ",
                                 obj.WriteReadable()));
              }
            }
          }
          return vec;
        } else {
          // TODO(CAJan93): same case as above. Simplify!
          jsonView sub_json = value.GetObject(key);
          without_cv_t sub_obj = from_json<without_cv_t>(sub_json);
          without_cv_t new_sub_obj{sub_obj};
          return new_sub_obj;
        }
      } else {
        Fail(JOIN_TO_STR("Unable to process key ", key, " Current JSON object is\n", value.WriteReadable()));
      }
    }
  }
  Fail("unreachable statement reached");
}

// alias for uint_32_t
template <>
inline void JsonSerializer::with_any<ChunkID>(jsonVal& data, const std::string& key, const ChunkID& val) {
  data.WithInteger(key, val);
}

// alias for uint_32_T
template <>
inline void JsonSerializer::with_any<ChunkOffset>(jsonVal& data, const std::string& key, const ChunkOffset& val) {
  data.WithInteger(key, val);
}

// alias for uint16_t
template <>
inline void JsonSerializer::with_any<ColumnID>(jsonVal& data, const std::string& key, const ColumnID& val) {
  data.WithInteger(key, val);
}

// alias for uint32_t
template <>
inline void JsonSerializer::with_any<CpuID>(jsonVal& data, const std::string& key, const CpuID& val) {
  data.WithInteger(key, val);
}

// alias for uint32_t
template <>
inline void JsonSerializer::with_any<NodeID>(jsonVal& data, const std::string& key, const NodeID& val) {
  data.WithInteger(key, val);
}

// alias for uint32_t
template <>
inline void JsonSerializer::with_any<ValueID>(jsonVal& data, const std::string& key, const ValueID& val) {
  data.WithInteger(key, val);
}

template <>
inline void JsonSerializer::with_any<bool>(jsonVal& data, const std::string& key, const bool& val) {
  data.WithBool(key, val);
}

template <>
inline void JsonSerializer::with_any<int>(jsonVal& data, const std::string& key, const int& val) {
  data.WithInteger(key, val);
}

template <>
inline void JsonSerializer::with_any<std::string>(jsonVal& data, const std::string& key, const std::string& val) {
  data.WithString(key, val);
}

template <>
inline void JsonSerializer::with_any<double>(jsonVal& data, const std::string& key, const double& val) {
  // TODO(CAJan93): Always Assert that key of type exists
  // TODO(CAJan93): Write convenience function
  // TODO(CAJan93): Maybe write WithType<T> this function then does the assertions?
  // TODO(CAJan93): Check policy Skyrise (assert vs. throw exception)
  data.WithDouble(key, val);
}

template <>
inline void JsonSerializer::with_any<AllTypeVariant>(jsonVal& data, const std::string& key, const AllTypeVariant& val) {
  const unsigned int val_t = val.which();
  jsonVal variant_jv;
  variant_jv.WithInteger("val_t", val_t);
  if (val_t == 0) {
    variant_jv.WithString("val", "NULL");
  } else if (val_t == 1) {
    variant_jv.WithInteger("val", boost::get<int>(val));
  } else if (val_t == 2) {
    variant_jv.WithInteger("val", boost::get<long int>(val));
  } else if (val_t == 3) {
    variant_jv.WithDouble("val", boost::get<float>(val));
  } else if (val_t == 4) {
    variant_jv.WithDouble("val", boost::get<double>(val));
  } else if (val_t == 5) {
    variant_jv.WithString("val", std::string(boost::get<string_t>(val)));
  }
  data.WithObject(key, variant_jv);
}

template <>
inline void JsonSerializer::with_any<std::variant<int, double, std::string>>(
    jsonVal& data, const std::string& key, const std::variant<int, double, std::string>& val) {
  switch (val.index()) {
    case 0:
      data.WithInteger(key, std::get<0>(val));
      break;

    case 1:
      data.WithDouble(key, std::get<1>(val));
      break;

    case 2:
      data.WithString(key, std::get<2>(val));
      break;

    default:
      Fail(JOIN_TO_STR("Error at key '", key,
                       "'. Json serializer only support variants with <int, double, std::string>",
                       "\n           Json was ", data.View().WriteReadable()));
  }
}

template <typename T>
inline void JsonSerializer::with_any(jsonVal& data, const std::string& key, const T& val) {
  if constexpr (std::is_pointer<T>::value) {
    if (val == nullptr) {
      data.WithString(key, "NULL");
    } else {
      // const AbstractOperator* const&
      typedef typename std::remove_reference_t<std::remove_cv_t<std::remove_pointer_t<T>>> without_ref_cv_ptr_t;
      if constexpr (has_member_properties<without_ref_cv_ptr_t>::value ||
                    std::is_same<without_ref_cv_ptr_t, AbstractExpression>::value ||
                    std::is_same<without_ref_cv_ptr_t, AbstractOperator>::value) {
        // nested (T::properties present)
        data.WithObject(key, JsonSerializer::to_json(val));
      } else {
        // non-nested (T::properties not present)
        with_any(data, key, *val);
      }
    }
    return;
  } else if constexpr (is_weak_ptr<T>::value) {
    with_any(data, key, val.lock().get());
  } else if constexpr (is_smart_ptr<T>::value) {
    StaticAssert<!is_unique_ptr<T>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");
    with_any(data, key, val.get());
  } else if constexpr (is_vector<T>::value) {
    data.WithObject(key, vec_to_json(val));
  } else if constexpr (std::is_enum<T>::value) {
    const std::string enum_name = magic_enum::enum_name(val).data();
    data.WithString(key, enum_name);
  } else {
    if constexpr (has_member_properties<T>::value) {
      // nested (T::properties present)
      data.WithObject(key, JsonSerializer::to_json(val));
    } else {
      with_any(data, key, val);
    }
  }
}

template <typename T>
jsonVal JsonSerializer::vec_to_json(const std::vector<T>& vec) {
  jsonVal jv;
  for (size_t idx = 0; idx < vec.size(); ++idx) {
    with_any<T>(jv, std::to_string(idx), vec.at(idx));
  }
  return jv;
}

// TODO(CAJan93): Use decltype(some_function(OperatorType o))
template <OperatorType>
struct get_operator_type;

template <>
struct get_operator_type<OperatorType::Alias> {
  using Type = AliasOperator;
};
template <>
struct get_operator_type<OperatorType::Limit> {
  using Type = Limit;
};
template <>
struct get_operator_type<OperatorType::Projection> {
  using Type = Projection;
};
template <>
struct get_operator_type<OperatorType::TableScan> {
  using Type = TableScan;
};

template <>
struct get_operator_type<OperatorType::Validate> {
  using Type = Validate;
};

// unserialize function
template <typename T>
T JsonSerializer::from_json(const jsonView& data) {
  typedef typename std::remove_cv_t<T> without_cv_t;

  if constexpr (is_smart_ptr<without_cv_t>::value) {
    StaticAssert<!is_unique_ptr<without_cv_t>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");
    StaticAssert<!is_weak_ptr<without_cv_t>::value>::stat_assert(
        "Weak pointers are currently not supported by this json serializer");
    std::cout << "is shared ptr" << std::endl;  // TODO(CAJan93): remove debug msg
    typedef T smart_ptr_t;                      // type of the smart pointer
    typedef get_inner_t<smart_ptr_t> inner_t;   // type of the object the pointer is pointing to
    inner_t* object = from_json<inner_t*>(data);
    smart_ptr_t sp = smart_ptr_t(object);
    return sp;
  } else {
    // pointer or object
    constexpr bool is_raw_ptr = std::is_pointer<without_cv_t>::value;
    typedef typename std::remove_pointer_t<without_cv_t> without_ptr_t;

    if constexpr (std::is_same<AbstractOperator, without_ptr_t>::value) {
      std::cout << "AbstractOperator" << std::endl;  // TODO(CAJan93) remove debug msg

      Assert(data.KeyExists("_type") || data.KeyExists("type"),
             JOIN_TO_STR("AbstractOperator needs type in order to be casted to concrete operator. Json was ",
                         data.WriteReadable()));

      // TODO(CAJan93): Do I really have to support _type and type? Not just _type?
      const std::string type_key = data.KeyExists("_type") ? "_type" : "type";
      auto operator_type_opt = magic_enum::enum_cast<OperatorType>(data.GetString(type_key));
      if (!operator_type_opt.has_value()) {
        Fail(JOIN_TO_STR("Unable to create OperatorType from string '", data.GetString(type_key), "'"));
      }

      const OperatorType operator_type = operator_type_opt.value();
      if (operator_type == OperatorType::Aggregate) {
        if (data.KeyExists("_has_aggregate_functions")) {
          std::cout << "AggregateHash" << std::endl;  //  TODO(CAJan93): Remove this debug msg
          return from_json<AggregateHash*>(data);
        } else {
          std::cout << "AggregateSort" << std::endl;  //  TODO(CAJan93): Remove this debug msg
          return from_json<AggregateSort*>(data);
        }
      }
      std::cout << "OperatorType: " << magic_enum::enum_name(operator_type).data()
                << std::endl;  // TODO(CAJan93): remove debug msg
      switch (operator_type) {
        case OperatorType::Alias:
          return from_json<AliasOperator*>(data);
        case OperatorType::GetTable:
          return from_json<GetTable*>(data);
        case OperatorType::Limit:
          return from_json<Limit*>(data);
        case OperatorType::Projection:
          return from_json<Projection*>(data);
        case OperatorType::TableScan:
          return from_json<TableScan*>(data);
        case OperatorType::Validate:
          return from_json<Validate*>(data);

        default:
          Fail(JOIN_TO_STR("Unsupported OperatorType '", data.GetString(type_key), "'"));
      }

    } else if constexpr (std::is_same<AbstractExpression, without_ptr_t>::value) {
      auto expression_type_opt = magic_enum::enum_cast<ExpressionType>(data.GetString("type"));
      if (!expression_type_opt.has_value()) {
        Fail(JOIN_TO_STR("Unable to create ExpressionType enum from string ", data.GetString("type")));
      }

      const ExpressionType expression_type = expression_type_opt.value();
      std::cout << "ExpressionType: " << magic_enum::enum_name(expression_type).data()
                << std::endl;  // TODO(CAJan93): remove debug msg

      switch (expression_type) {
        case ExpressionType::Aggregate:
          return from_json<AggregateExpression*>(data);
        case ExpressionType::Arithmetic:
          return from_json<ArithmeticExpression*>(data);
        case ExpressionType::Cast:
          return from_json<CastExpression*>(data);
        case ExpressionType::Case:
          return from_json<CaseExpression*>(data);
        case ExpressionType::CorrelatedParameter:
          return from_json<CorrelatedParameterExpression*>(data);
        case ExpressionType::PQPColumn:
          return from_json<PQPColumnExpression*>(data);
        case ExpressionType::LQPColumn:
          Fail("Serializer does not support LQPColumn expression");
        case ExpressionType::Exists:
          return from_json<ExistsExpression*>(data);
        case ExpressionType::Extract:
          return from_json<ExtractExpression*>(data);
        case ExpressionType::Function:
          return from_json<FunctionExpression*>(data);
        case ExpressionType::List:
          return from_json<ListExpression*>(data);
        case ExpressionType::Logical:
          return from_json<LogicalExpression*>(data);
        case ExpressionType::Placeholder:
          return from_json<PlaceholderExpression*>(data);
        case ExpressionType::Predicate: {
          Assert(data.KeyExists("type"),
                 JOIN_TO_STR("Json does not contain key 'type' required to cast AbstractPredicateExpression to "
                             "concrete type\nJson is.\n           ",
                             data.WriteReadable()));
          Assert(data.GetObject("type").IsString(),
                 JOIN_TO_STR("Json does contain key 'type' required to cast AbstractPredicateExpression to "
                             "concrete type, but 'type' is not a string.\nJson is\n           ",
                             data.WriteReadable()));
          const std::string predicate_type = data.GetString("predicate_condition");
          const auto predicate_contition_opt = magic_enum::enum_cast<PredicateCondition>(predicate_type);
          // TODO(CAJan93): Write these statements as Asserts
          if (!predicate_contition_opt.has_value()) {
            Fail(JOIN_TO_STR("Unable to create enum of type PredicateCondition from string '", predicate_type, "'"));
          }
          const PredicateCondition pred_cond = predicate_contition_opt.value();

          switch (pred_cond) {
            // TODO(CAJan93): simplify this switch case: is_between_expression, is_binary_pred_expr, ...
            case PredicateCondition::BetweenExclusive:
            case PredicateCondition::BetweenInclusive:
            case PredicateCondition::BetweenLowerExclusive:
            case PredicateCondition::BetweenUpperExclusive: {
              std::cout << "between expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              return from_json<BetweenExpression*>(data);
            }

            // TODO(CAJan93): Is this correct? Does the binary pred. expr. cover all these cases?
            case PredicateCondition::Equals:
            case PredicateCondition::GreaterThan:
            case PredicateCondition::GreaterThanEquals:
            case PredicateCondition::LessThan:
            case PredicateCondition::LessThanEquals:
            case PredicateCondition::Like:
            case PredicateCondition::NotEquals:
            case PredicateCondition::NotLike: {
              std::cout << "binary predicate expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              return from_json<BinaryPredicateExpression*>(data);
            }

            case PredicateCondition::In:
            case PredicateCondition::NotIn: {
              std::cout << "in expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              return from_json<InExpression*>(data);
            }

            case PredicateCondition::IsNotNull:
            case PredicateCondition::IsNull: {
              std::cout << "is null expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              return from_json<IsNullExpression*>(data);
            }

            default:
              Fail("Unknown ExpressionType\n");
          }
          // TODO(CAJan93): Implement AbstractPredicateExpression
          Fail("AbstractPredicateExpression currently not supported");
        }

        case ExpressionType::PQPSubquery:
          return from_json<PQPSubqueryExpression*>(data);
        case ExpressionType::LQPSubquery:
          Fail("Serializer does not support LQPSubquery expression");
        case ExpressionType::UnaryMinus:
          return from_json<UnaryMinusExpression*>(data);
        case ExpressionType::Value:
          return from_json<ValueExpression*>(data);
        default:
          Fail(JOIN_TO_STR("ExpressionType: ", magic_enum::enum_name(expression_type).data()));
      }
      Fail("not implemented");
    } else if constexpr (std::is_same<AggregateExpression, without_ptr_t>::value ||
                         std::is_same<IsNullExpression, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      typedef typename decltype(property_0)::Type property_0_t;

      constexpr auto property_1 = std::get<1>(without_ptr_t::properties);
      typedef typename decltype(property_1)::Type property_1_t;

      if constexpr (is_raw_ptr) {
        without_cv_t t = new without_ptr_t(as_any<property_0_t>(data, property_0.name),
                                           as_any<property_1_t>(data, property_1.name).at(0));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name).at(0));
        return t;
      }
    } else if constexpr (std::is_same<ArithmeticExpression, without_ptr_t>::value ||
                         std::is_same<BinaryPredicateExpression, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      typedef typename decltype(property_0)::Type property_0_t;

      constexpr auto property_1 = std::get<1>(without_ptr_t::properties);
      typedef typename decltype(property_1)::Type property_1_t;

      if constexpr (is_raw_ptr) {
        without_cv_t t = new without_ptr_t(as_any<property_0_t>(data, property_0.name),
                                           as_any<property_1_t>(data, property_1.name).at(0),
                                           as_any<property_1_t>(data, property_1.name).at(1));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name).at(0),
                        as_any<property_1_t>(data, property_1.name).at(1));
        return t;
      }
    } else if constexpr (std::is_same<BetweenExpression, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      typedef typename decltype(property_0)::Type property_0_t;

      constexpr auto property_1 = std::get<1>(without_ptr_t::properties);
      typedef typename decltype(property_1)::Type property_1_t;
      if constexpr (is_raw_ptr) {
        without_cv_t t = new without_ptr_t(
            as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name).at(0),
            as_any<property_1_t>(data, property_1.name).at(1), as_any<property_1_t>(data, property_1.name).at(2));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name).at(0),
                        as_any<property_1_t>(data, property_1.name).at(1),
                        as_any<property_1_t>(data, property_1.name).at(2));
        return t;
      }
    } else if constexpr (std::is_same<PQPColumnExpression, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      typedef typename decltype(property_0)::Type property_0_t;

      constexpr auto property_1 = std::get<1>(without_ptr_t::properties);
      typedef typename decltype(property_1)::Type property_1_t;

      constexpr auto property_2 = std::get<2>(without_ptr_t::properties);
      typedef typename decltype(property_2)::Type property_2_t;

      constexpr auto property_3 = std::get<3>(without_ptr_t::properties);
      typedef typename decltype(property_3)::Type property_3_t;

      if constexpr (is_raw_ptr) {
        without_cv_t t =
            new without_ptr_t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name),
                              as_any<property_2_t>(data, property_2.name), as_any<property_3_t>(data, property_3.name));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name),
                        as_any<property_2_t>(data, property_2.name), as_any<property_3_t>(data, property_3.name));
        return t;
      }
    } else if constexpr (std::is_same<ValueExpression, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      typedef typename decltype(property_0)::Type property_0_t;

      if constexpr (is_raw_ptr) {
        without_cv_t t = new without_ptr_t(as_any<property_0_t>(data, property_0.name));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name));
        return t;
      }
    } else if constexpr (std::is_same<GetTable, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      typedef typename decltype(property_0)::Type property_0_t;

      constexpr auto property_1 = std::get<1>(without_ptr_t::properties);
      typedef typename decltype(property_1)::Type property_1_t;

      constexpr auto property_2 = std::get<2>(without_ptr_t::properties);
      typedef typename decltype(property_2)::Type property_2_t;

      if constexpr (is_raw_ptr) {
        without_cv_t t =
            new without_ptr_t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name),
                              as_any<property_2_t>(data, property_2.name));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name),
                        as_any<property_2_t>(data, property_2.name));
        return t;
      }
    }

    else if constexpr (std::is_same<AggregateHash, without_ptr_t>::value ||
                       std::is_same<AggregateSort, without_ptr_t>::value ||
                       std::is_same<AliasOperator, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      // typedef typename decltype(property_0) property_0_t; // TODO(CAJan93): do not hardcode this
      typedef typename std::shared_ptr<AbstractOperator> property_0_t;

      constexpr auto property_1 = std::get<1>(without_ptr_t::properties);
      typedef typename decltype(property_1)::Type property_1_t;

      constexpr auto property_2 = std::get<2>(without_ptr_t::properties);
      typedef typename decltype(property_2)::Type property_2_t;

      if constexpr (is_raw_ptr) {
        without_cv_t t =
            new without_ptr_t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name),
                              as_any<property_2_t>(data, property_2.name));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name),
                        as_any<property_2_t>(data, property_2.name));
        return t;
      }
    } else if constexpr (std::is_same<Projection, without_ptr_t>::value ||
                         std::is_same<TableScan, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      typedef typename decltype(property_0)::Type property_0_t;

      constexpr auto property_1 = std::get<1>(without_ptr_t::properties);
      typedef typename decltype(property_1)::Type property_1_t;
      if constexpr (is_raw_ptr) {
        without_cv_t t =
            new without_ptr_t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name), as_any<property_1_t>(data, property_1.name));
        return t;
      }
    } else if constexpr (std::is_same<Validate, without_ptr_t>::value) {
      constexpr auto property_0 = std::get<0>(without_ptr_t::properties);
      // typedef typename decltype(property_0) property_0_t; // TODO(CAJan93): do not hardcode this
      typedef typename std::shared_ptr<AbstractOperator> property_0_t;

      if constexpr (is_raw_ptr) {
        without_cv_t t = new without_ptr_t(as_any<property_0_t>(data, property_0.name));
        return t;
      } else {
        without_ptr_t t(as_any<property_0_t>(data, property_0.name));
        return t;
      }
    } else if constexpr (std::is_same<T, AllTypeVariant>::value) {
      return as_any<AllTypeVariant>(data, "value");
    } else {
      Fail(JOIN_TO_STR("Unsupported type", typeid(T).name(), " in json deserialization"));
    }
  }
}  // namespace opossum

template <typename T>
jsonVal JsonSerializer::to_json(const T& object) {
  jsonVal data;
  typedef typename std::remove_cv_t<T> without_cv;
  typedef typename std::remove_reference_t<without_cv> without_ref_cv_t;

  if constexpr (std::is_pointer<without_ref_cv_t>::value) {
    if constexpr (std::is_same<without_ref_cv_t, const AbstractOperator*>::value) {
      // cast Abstract operators
      auto abstract_op = (const AbstractOperator*)object;
      switch (abstract_op->type()) {
        case OperatorType::Alias: {
          const auto alias = dynamic_cast<const AliasOperator*>(abstract_op);
          std::cout << "AliasOperator" << std::endl;  //  TODO(CAJan93): Remove this debug msg
          return to_json<AliasOperator>(*alias);
        }

        case OperatorType::Aggregate: {
          const auto abstract_agg = dynamic_cast<const AbstractAggregateOperator*>(abstract_op);
          if (const auto agg_hash = dynamic_cast<const AggregateHash*>(abstract_agg); agg_hash) {
            std::cout << "Aggregate Hash" << std::endl;  //  TODO(CAJan93): Remove this debug msg
            return to_json<AggregateHash>(*agg_hash);
          } else if (const auto agg_sort = dynamic_cast<const AggregateSort*>(abstract_agg); agg_sort) {
            // TODO(CAJan93): Test this path
            std::cout << "Aggregate Sort" << std::endl;  //  TODO(CAJan93): Remove this debug msg
            return to_json<AggregateSort>(*agg_sort);
          }
          Fail("Unable to cast AbastractAggregator to concrete instance");
          return data;
        }

        case OperatorType::GetTable: {
          const auto gt = dynamic_cast<const GetTable*>(abstract_op);
          std::cout << "GetTable" << std::endl;  //  TODO(CAJan93): Remove this debug msg
          return to_json<GetTable>(*gt);
        }

        // TODO(CAJan93): Check if I am using the corret limit class
        case OperatorType::Limit: {
          const auto limit = dynamic_cast<const Limit*>(abstract_op);
          std::cout << "limit" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<Limit>(*limit);
        }

        case OperatorType::Projection: {
          const auto projection = dynamic_cast<const Projection*>(abstract_op);
          std::cout << "projection" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<Projection>(*projection);
        }

        case OperatorType::TableScan: {
          const auto table_scan = dynamic_cast<const TableScan*>(abstract_op);
          std::cout << "TableScan" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<TableScan>(*table_scan);
        }

        case OperatorType::Validate: {
          const auto validate = dynamic_cast<const Validate*>(abstract_op);
          std::cout << "Validate" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<Validate>(*validate);
          return data;
        }

        default: {
          // TODO(CAJan93) remove below code
          auto t = abstract_op->type();
          std::cout << "default OperatorType, with type \n           " << magic_enum::enum_name(t).data() << '\n';
        }
      }
      return data;

    } else if constexpr (std::is_same<without_ref_cv_t, AbstractExpression*>::value) {
      switch (object->type) {
          // TODO(CAJan93): Support the other ExpressionTypes

        case ExpressionType::Arithmetic: {
          const auto arithmetic_expr = dynamic_cast<ArithmeticExpression*>(object);
          std::cout << "Arithmetic expression" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<ArithmeticExpression>(*arithmetic_expr);
        }

        case ExpressionType::PQPColumn: {
          const auto pqp_col = dynamic_cast<PQPColumnExpression*>(object);
          std::cout << "PQPColumn expression" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<PQPColumnExpression>(*pqp_col);
        }

        case ExpressionType::Predicate: {
          const auto pred = dynamic_cast<AbstractPredicateExpression*>(object);
          std::cout << "abstract predicate" << std::endl;  // TODO(CAJan93): Remove debug msg
          switch (pred->predicate_condition) {
            case PredicateCondition::BetweenExclusive:
            case PredicateCondition::BetweenInclusive:
            case PredicateCondition::BetweenLowerExclusive:
            case PredicateCondition::BetweenUpperExclusive: {
              std::cout << "between expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              const auto pred_between = dynamic_cast<BetweenExpression*>(object);
              return to_json<BetweenExpression>(*pred_between);
            }

            // TODO(CAJan93): Is this correct? Does the binary pred. expr. cover all these cases?
            case PredicateCondition::Equals:
            case PredicateCondition::GreaterThan:
            case PredicateCondition::GreaterThanEquals:
            case PredicateCondition::LessThan:
            case PredicateCondition::LessThanEquals:
            case PredicateCondition::Like:
            case PredicateCondition::NotEquals:
            case PredicateCondition::NotLike: {
              std::cout << "binary predicate expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              const auto pred_binary = dynamic_cast<BinaryPredicateExpression*>(object);
              return to_json<BinaryPredicateExpression>(*pred_binary);
            }

            case PredicateCondition::In:
            case PredicateCondition::NotIn: {
              std::cout << "in expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              const auto pred_in = dynamic_cast<InExpression*>(object);
              return to_json<InExpression>(*pred_in);
            }

            case PredicateCondition::IsNotNull:
            case PredicateCondition::IsNull: {
              std::cout << "is null expression" << std::endl;  // TODO(CAJan93): Remove debug msg
              const auto pred_null = dynamic_cast<IsNullExpression*>(object);
              return to_json<IsNullExpression>(*pred_null);
            }

            default:
              Fail("Unknown ExpressionType\n");
              return data;
          }
        }

        case ExpressionType::LQPColumn: {
          Fail("JsonSerializer does not support ExpressionType::LQPColumn");  // TODO(CAJan93): remove this?
          return data;
        }

        case ExpressionType::Value: {
          const auto val_expr = dynamic_cast<ValueExpression*>(object);
          std::cout << "Value expression\n";  // TODO(CAJan93): remove debug msg
          return to_json<ValueExpression>(*val_expr);
        }

        default:
          // TODO(CAJan93): Handle the other ExpressionTypes
          std::cout << "Failure. Unsupported ExpressionType " << magic_enum::enum_name(object->type).data() << '\n';
          return data;
          break;
      }

    } else if constexpr (std::is_same<without_ref_cv_t, const AbstractLQPNode*>::value) {
      Fail("JsonSerializer does not support ExpressionType::AbstractLQPNode");  // TODO(CAJan93): remove this?
      return data;
    }

    else {
      return to_json<std::remove_pointer_t<without_ref_cv_t>>(*object);
    }
  } else if constexpr (is_weak_ptr<without_ref_cv_t>::value) {
    return to_json(object.lock().get());
  } else if constexpr (is_smart_ptr<without_ref_cv_t>::value) {
    StaticAssert<!is_unique_ptr<without_ref_cv_t>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");
    return to_json(object.get());  // keep const qualifier, since get() might return a const pointer
  } else if constexpr (has_member_properties<without_ref_cv_t>::value) {
    // serialize a class that provides properties tuple
    constexpr auto nb_properties = std::tuple_size<decltype(without_ref_cv_t::properties)>::value;
    for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      constexpr auto property = std::get<i>(without_ref_cv_t::properties);
      with_any(data, property.name, object.*(property.member));
    });
    return data;
  } else if constexpr (has_member__type<without_ref_cv_t>::value) {
    // TODO(CAJan93): remove this case? If so, remove has_member__type?
    std::cout << "type is " << magic_enum::enum_name(object.type()).data() << " is currently not supported.\n";
    return data;
  } else {
    Fail(JOIN_TO_STR("\nunsupported type ", typeid(object).name(), "\n           typeid T: ", typeid(T).name(),
                     "\n           typeid without_ref_cv_t: ", typeid(without_ref_cv_t).name()));
  }
  return data;
}  // namespace opossum

// TODO(CAJan93): remove this?
template <typename T>
std::string JsonSerializer::to_json_str(const T& object) {
  return to_json(object).View().WriteReadable();
}

template <typename T>
auto JsonSerializer::from_json_str(const std::string& json_str) {
  const jsonVal data(json_str);
  const jsonView jv = data.View();
  return from_json<T>(jv);
}
}  // namespace opossum
