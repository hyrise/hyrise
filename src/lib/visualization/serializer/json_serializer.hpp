#pragma once

#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <variant>

#include <aws/core/utils/json/JsonSerializer.h>

// TODO(CAJan93): #include "../assert.hpp"
// TODO(CAJan93): #include "../string.hpp"
// TODO(CAJan93): #include "assert.hpp"
// TODO(CAJan93): #include "../types/types.hpp"
#include "../../expression/pqp_column_expression.hpp"
#include "../../logical_query_plan/abstract_lqp_node.hpp"
#include "../../operators/abstract_operator.hpp"
#include "../../operators/limit.hpp"
#include "../../operators/projection.hpp"
#include "../../operators/table_scan.hpp"
#include "../../operators/validate.hpp"
#include "../../types.hpp"
#include "../../utils/assert.hpp"
#include "../types/get_inner_type.hpp"
#include "../types/is_smart_ptr.hpp"
#include "../types/is_vector.hpp"
#include "has_member.hpp"
#include "string.hpp"

namespace opossum {

// forward declaration and aliases
class AbstractLQPNode;
class PQPColumnExpression;
class TableScan;
class Validate;
using jsonVal = Aws::Utils::Json::JsonValue;
using jsonView = Aws::Utils::Json::JsonView;

// TODO(CAJan93): remove and use magic enum
inline std::string print_type(const OperatorType& t) {
  switch (t) {
    case OperatorType::Aggregate:
      return "Aggregate";
    case OperatorType::Alias:
      return "Alias";
    case OperatorType::ChangeMetaTable:
      return "ChangeMetaTable";
    case OperatorType::CreateTable:
      return "CreateTable";
    case OperatorType::CreatePreparedPlan:
      return "CreatePreparedPlan";
    case OperatorType::CreateView:
      return "CreateView";
    case OperatorType::DropTable:
      return "DropTable";
    case OperatorType::DropView:
      return "DropView";
    case OperatorType::Delete:
      return "Delete";
    case OperatorType::Difference:
      return "Difference";
    case OperatorType::Export:
      return "Export";
    case OperatorType::GetTable:
      return "GetTable";
    case OperatorType::Import:
      return "Import";
    case OperatorType::IndexScan:
      return "IndexScan";
    case OperatorType::Insert:
      return "Insert";
    case OperatorType::JoinHash:
      return "JoinHash";
    case OperatorType::JoinIndex:
      return "JoinIndex";
    case OperatorType::JoinNestedLoop:
      return "JoinNestedLoop";
    case OperatorType::JoinSortMerge:
      return "JoinSortMerge";
    case OperatorType::JoinVerification:
      return "JoinVerification";
    case OperatorType::Limit:
      return "Limit";
    case OperatorType::Print:
      return "Print";
    case OperatorType::Product:
      return "Product";
    case OperatorType::Projection:
      return "Projection";
    case OperatorType::Sort:
      return "Sort";
    case OperatorType::TableScan:
      return "TableScan";
    case OperatorType::TableWrapper:
      return "TableWrapper";
    case OperatorType::UnionAll:
      return "UnionAll";
    case OperatorType::UnionPositions:
      return "UnionPositions";
    case OperatorType::Update:
      return "Update";
    case OperatorType::Validate:
      return "Validate";
    default:
      return "mock";
  }
}

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

 public:
  // unserialize function
  template <typename T>
  static T from_json(const jsonView& data);

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
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsIntegerType(),
              JOIN_TO_STR("key ", key, " is not an integer type\n", "JSON: ", value.WriteReadable()));
  return value.GetInteger(key);
}

template <>
inline ChunkOffset JsonSerializer::as_any<ChunkOffset>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsIntegerType(),
              JOIN_TO_STR("key ", key, " is not an integer type\n", "JSON: ", value.WriteReadable()));
  return value.GetInteger(key);
}

template <>
inline std::string JsonSerializer::as_any<std::string>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsString(),
              JOIN_TO_STR("key ", key, " is not of type string\n", "JSON: ", value.WriteReadable()));
  return value.GetString(key);
}

template <>
inline double JsonSerializer::as_any<double>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsFloatingPointType(),
              JOIN_TO_STR("key ", key, " is not a floting point type\n", "JSON: ", value.WriteReadable()));
  return value.GetDouble(key);
}

template <>
inline std::variant<int, double, std::string> JsonSerializer::as_any<std::variant<int, double, std::string>>(
    const jsonView& value, const std::string& key) {
  if (value.GetObject(key).IsIntegerType()) {
    return value.GetInteger(key);
  }
  if (value.GetObject(key).IsFloatingPointType()) {
    return value.GetDouble(key);
  }
  if (value.GetObject(key).IsString()) {
    return value.GetString(key);
  }
  Fail("json deserializer only support variants with <int, double, string>");
  return -1;
}

// for non-trivial objects
template <typename T>
inline T JsonSerializer::as_any(const jsonView& value, const std::string& key) {
  typedef typename std::remove_cv_t<T> without_cv_t;
  typedef typename std::remove_pointer_t<without_cv_t> without_cv_value_t;

  if constexpr (std::is_pointer<without_cv_t>::value) {
    // nullpointers
    if (value.GetObject(key).IsString() && value.GetString(key) == "NULL") {
      return nullptr;
    }
    if (value.GetObject(key).IsObject()) {
      // handle nested object (pointer)
      jsonView sub_json = value.GetObject(key);
      if constexpr (has_member_properties<without_cv_value_t>::value) {
        // T:: properties exist
        without_cv_t sub_obj = from_json<without_cv_t>(sub_json);
        return sub_obj;
      }
      Fail(JOIN_TO_STR("Unable to process key ", key, "\nKey is is refering to an object of type ", typeid(T).name(),
                       "*. Current JSON object is ", value.WriteReadable()));
    } else {
      without_cv_value_t sub_obj = as_any<without_cv_value_t>(value, key);
      without_cv_value_t* new_sub_obj = new without_cv_value_t{sub_obj};
      return new_sub_obj;
    }
  } else if constexpr (is_smart_ptr<without_cv_t>::value) {
    StaticAssert<!is_weak_ptr<without_cv_t>::value>::stat_assert(
        "Weak pointers are currently not supported by this json serializer");
    StaticAssert<!is_unique_ptr<without_cv_t>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");

    typedef typename std::remove_cv_t<without_cv_t> smart_ptr_t;
    typedef get_inner_t<smart_ptr_t> inner_t;            // type of the object the pointer is pointing to
    inner_t* object_ptr = as_any<inner_t*>(value, key);  // a pointer to such an object

    return smart_ptr_t(object_ptr);
  } else {
    if constexpr (std::is_enum<without_cv_t>::value) {
      if (value.GetObject(key).IsIntegerType()) {
        // handle enum
        return static_cast<without_cv_t>(value.GetInteger(key));
      }
    } else {
      if (value.GetObject(key).IsObject()) {
        // handle nested object (pointer or non-pointer)
        jsonView sub_json = value.GetObject(key);
        if constexpr (has_member_properties<without_cv_t>::value) {
          without_cv_t sub_obj = from_json<without_cv_t>(sub_json);
          without_cv_t new_sub_obj{sub_obj};
          return new_sub_obj;
        } else {
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
            StaticAssert<!is_weak_ptr<T>::value>::stat_assert(
                "Weak pointers are currently not supported by this json serializer");
            StaticAssert<!is_unique_ptr<T>::value>::stat_assert(
                "Unique pointers are currently not supported by this json serializer");
            Fail("smart pointers are not handled");
          } else {
            for (size_t idx = 0; obj.KeyExists(std::to_string(idx)); ++idx) {
              const jsonView data = obj.GetObject(std::to_string(idx));
              if constexpr (has_member_properties<without_cv_vec_inner_t>::value) {
                vec.emplace_back(from_json<without_cv_vec_inner_t>(
                    data));  // serializer only supports vectors and no other containers
              } else if constexpr (std::is_same<without_cv_vec_inner_t, int>::value) {
                vec.emplace_back(data.AsInteger());
              } else if constexpr (std::is_same<without_cv_vec_inner_t, double>::value) {
                vec.emplace_back(data.AsDouble());
              } else if constexpr (std::is_same<without_cv_vec_inner_t, std::string>::value) {
                vec.emplace_back(data.AsString());
              } else if constexpr (std::is_same<without_cv_vec_inner_t, bool>::value) {
                vec.emplace_back(data.AsBool());
              } else {
                Fail("Unsupported vector type");
              }
            }
          }
          return vec;
        }
      } else {
        Fail(JOIN_TO_STR("Unable to process key ", key, " Current JSON object is ", value.WriteReadable()));
      }
    }
  }
  Fail("unreachable statement reached");
}

// ColumnID alias for uint16_t
template <>
inline void JsonSerializer::with_any<ColumnID>(jsonVal& data, const std::string& key, const ColumnID& val) {
  data.WithInteger(key, val);
}

// ChunkID alias for uint_32_t
template <>
inline void JsonSerializer::with_any<ChunkID>(jsonVal& data, const std::string& key, const ChunkID& val) {
  data.WithInteger(key, val);
}

// ValueID alias for uint32_t
template <>
inline void JsonSerializer::with_any<ValueID>(jsonVal& data, const std::string& key, const ValueID& val) {
  data.WithInteger(key, val);
}

// NodeID alias for uint32_t
template <>
inline void JsonSerializer::with_any<NodeID>(jsonVal& data, const std::string& key, const NodeID& val) {
  data.WithInteger(key, val);
}

// CpuID alias for uint32_t
template <>
inline void JsonSerializer::with_any<CpuID>(jsonVal& data, const std::string& key, const CpuID& val) {
  data.WithInteger(key, val);
}

// TODO(CAJan93): implement JsonSerializer::with_any for ColumnCount (see types.hpp)
// STRONG_TYPEDEF(opossum::ColumnID::base_type, ColumnCount);

// TODO(CAJan93): implement as_any<bool>
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
  data.WithDouble(key, val);
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
                       "'. Json serializer only support variants with <int, double, std::string>", "\nJson was ",
                       data.View().WriteReadable()));
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
                    std::is_same<without_ref_cv_ptr_t, AbstractOperator>::value ||
                    std::is_same<without_ref_cv_ptr_t, AbstractLQPNode>::value) {
        // nested (T::properties present)
        data.WithObject(key, JsonSerializer::to_json(val));
      } else {
        // non-nested (T::properties not present)
        with_any(data, key, *val);
      }
    }
    return;
  } else if constexpr (is_smart_ptr<T>::value) {
    StaticAssert<!is_weak_ptr<T>::value>::stat_assert(
        "Weak pointers are currently not supported by this json serializer");
    StaticAssert<!is_unique_ptr<T>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");
    with_any(data, key, val.get());
  } else if constexpr (is_vector<T>::value) {
    data.WithObject(key, vec_to_json(val));
  } else if constexpr (std::is_enum<T>::value) {
    data.WithInteger(key, static_cast<int>(val));
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

// unserialize function
template <typename T>
T JsonSerializer::from_json(const jsonView& data) {
  typedef typename std::remove_cv_t<T> without_cv_t;

  if constexpr (std::is_pointer<without_cv_t>::value) {
    // check if we have nullptr
    if (data.IsString() && data.AsString() == "NULL") return nullptr;

    T object = new std::remove_pointer_t<without_cv_t>;
    // number of properties
    constexpr auto nb_properties = std::tuple_size<decltype(std::remove_pointer_t<without_cv_t>::properties)>::value;
    for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      // get the property
      constexpr auto property = std::get<i>(std::remove_pointer_t<without_cv_t>::properties);

      // get the type of the property
      using Type = typename decltype(property)::Type;

      // set the value to the member
      object->*(property.member) = as_any<Type>(data, property.name);
    });
    // call copy constructor to enable inheritance
    T new_object{object};

    return new_object;
  } else if constexpr (is_smart_ptr<without_cv_t>::value) {
    StaticAssert<!is_weak_ptr<without_cv_t>::value>::stat_assert(
        "Weak pointers are currently not supported by this json serializer");
    StaticAssert<!is_unique_ptr<without_cv_t>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");
    typedef T smart_ptr_t;                     // type of the smart pointer
    typedef get_inner_t<smart_ptr_t> inner_t;  // type of the object the pointer is pointing to
    inner_t* object = new inner_t;
    // number of properties
    constexpr auto nb_properties = std::tuple_size<decltype(inner_t::properties)>::value;
    for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      // get the property
      constexpr auto property = std::get<i>(inner_t::properties);

      // get the type of the property
      using Type = typename decltype(property)::Type;

      // set the value to the member
      object->*(property.member) = as_any<Type>(data, property.name);
    });
    // call copy constructor to enable inheritance
    smart_ptr_t sp = smart_ptr_t(object);
    return sp;
  } else {
    T object;
    constexpr auto nb_properties = std::tuple_size<decltype(T::properties)>::value;
    for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      constexpr auto property = std::get<i>(T::properties);
      using Type = typename decltype(property)::Type;
      object.*(property.member) = as_any<Type>(data, property.name);
    });
    // call copy constructor to enable inheritance
    T new_object{object};
    return new_object;
  }
}

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
        case OperatorType::Projection: {
          const auto projection = dynamic_cast<const Projection*>(abstract_op);
          std::cout << "projection" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<Projection>(*projection);
        } break;

        case OperatorType::TableScan: {
          const auto table_scan = dynamic_cast<const TableScan*>(abstract_op);
          std::cout << "TableScan" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<TableScan>(*table_scan);
        } break;

        case OperatorType::Limit: {
          const auto limit = dynamic_cast<const Limit*>(abstract_op);
          std::cout << "limit" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<Limit>(*limit);
        } break;

        case OperatorType::Validate: {
          const auto validate = dynamic_cast<const Validate*>(abstract_op);
          std::cout << "Validate" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<Validate>(*validate);
          return data;
        } break;

          /**
         * cast via AbstractAggregateOperator
        case OperatorType::Aggregate: {
          const auto aggregate = dynamic_cast<const Aggregate*>(abstract_op);
          std::cout << "aggregate\n";
          return to_json<Aggregate>(*aggregate);
        } break;
        */

        default: {
          std::cout << "default\n" << std::endl;
          // TODO(CAJan93) remove below code
          auto t = abstract_op->type();
          std::cout << "type is " << print_type(t) << '\n';  // TODO(CAJan93): Remove this debug msg
        }                                                    // OperatorType has no expressions
      }
      // TODO remove this line
      return data;
    } else if constexpr (std::is_same<without_ref_cv_t, AbstractExpression*>::value) {
      switch (object->type) {
        case ExpressionType::PQPColumn: {
          const auto pqp_col = dynamic_cast<PQPColumnExpression*>(object);
          std::cout << "PQPColumn" << std::endl;  // TODO(CAJan93): Remove this debug msg
          return to_json<PQPColumnExpression>(*pqp_col);
        } break;

        default:
          // TODO(CAJan93): Handle the other ExpressionTypes
          Fail(JOIN_TO_STR("Unsupported expression type"));
          break;
      }

    } else if constexpr (std::is_same<without_ref_cv_t, const AbstractLQPNode*>::value) {
      // TODO(CAJan93): Support AbstractLQPNode
      std::cout << "AbstractLQPNode currently not supported\n";
      return data;
    }

    else {
      return to_json<std::remove_pointer_t<without_ref_cv_t>>(*object);
    }
  } else if constexpr (is_smart_ptr<without_ref_cv_t>::value) {
    StaticAssert<!is_weak_ptr<without_ref_cv_t>::value>::stat_assert(
        "Weak pointers are currently not supported by this json serializer");
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
    std::cout << "type is " << print_type(object.type()) << " is currently not supported.\n";
    return data;
  } else {
    Fail(JOIN_TO_STR("\nunsupported type ", typeid(object).name(), "\ntypeid T: ", typeid(T).name(),
                     "\ntypeid without_ref_cv_t: ", typeid(without_ref_cv_t).name()));
  }
  return data;
}  // namespace opossum

// TODO(CAJan93): remove this?
template <typename T>
std::string JsonSerializer::to_json_str(const T& object) {
  return to_json(object).View().WriteReadable();
}
}  // namespace opossum