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
#include "../../utils/assert.hpp"
#include "../types/get_inner_type.hpp"
#include "../types/is_smart_ptr.hpp"
#include "../types/is_vector.hpp"
#include "class_has_properties_member.hpp"
#include "string.hpp"

namespace opossum {

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

using jsonVal = Aws::Utils::Json::JsonValue;
using jsonView = Aws::Utils::Json::JsonView;

template <typename T>
jsonVal to_json(const T& object);

template <typename T>
T from_json(const jsonView& data);

namespace details {
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
jsonVal vec_to_json(const std::vector<T>& vec);

// sequence for
template <typename T, T... S, typename F>
constexpr void for_sequence(std::integer_sequence<T, S...>, F&& f) {
  using unpack_t = int[];
  (void)unpack_t{(static_cast<void>(f(std::integral_constant<T, S>{})), 0)..., 0};
}

template <typename T>
T as_any(const jsonView&, const std::string&);

template <>
inline int as_any<int>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsIntegerType(),
              JOIN_TO_STR("key ", key, " is not an integer type\n", "JSON: ", value.WriteReadable()));
  return value.GetInteger(key);
}

template <>
inline ChunkOffset as_any<ChunkOffset>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsIntegerType(),
              JOIN_TO_STR("key ", key, " is not an integer type\n", "JSON: ", value.WriteReadable()));
  return value.GetInteger(key);
}

template <>
inline std::string as_any<std::string>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsString(),
              JOIN_TO_STR("key ", key, " is not of type string\n", "JSON: ", value.WriteReadable()));
  return value.GetString(key);
}

template <>
inline double as_any<double>(const jsonView& value, const std::string& key) {
  AssertInput(value.KeyExists(key), JOIN_TO_STR("key ", key, " does not exist\n", "JSON: ", value.WriteReadable()));
  AssertInput(value.GetObject(key).IsFloatingPointType(),
              JOIN_TO_STR("key ", key, " is not a floting point type\n", "JSON: ", value.WriteReadable()));
  return value.GetDouble(key);
}

template <>
inline std::variant<int, double, std::string> as_any<std::variant<int, double, std::string>>(const jsonView& value,
                                                                                             const std::string& key) {
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
inline T as_any(const jsonView& value, const std::string& key) {
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
      if constexpr (has_properties<without_cv_value_t>::value) {
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
    smart_ptr_t s_ptr_obj;
    typedef decltype(get_inner_t(s_ptr_obj)) inner_t;    // type of the object the pointer is pointing to
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
        if constexpr (has_properties<without_cv_t>::value) {
          without_cv_t sub_obj = from_json<without_cv_t>(sub_json);
          without_cv_t new_sub_obj{sub_obj};
          return new_sub_obj;
        } else {
          // deserialize a vector
          const jsonView obj = value.GetObject(key);
          without_cv_t vec;
          typedef decltype(get_inner_vec_t(vec)) vec_inner_t;
          typedef std::remove_cv_t<vec_inner_t> without_cv_vec_inner_t;

          if constexpr (std::is_pointer<without_cv_vec_inner_t>::value) {
            typedef std::remove_pointer_t<without_cv_vec_inner_t> without_ptr_without_cv_vec_inner_t;
            for (size_t idx = 0; obj.KeyExists(std::to_string(idx)); ++idx) {
              const jsonView data = obj.GetObject(std::to_string(idx));
              if constexpr (has_properties<without_ptr_without_cv_vec_inner_t>::value) {
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
              if constexpr (has_properties<without_cv_vec_inner_t>::value) {
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

template <typename T>
void with_any(jsonVal& data, const std::string& key, const T& val);

template <>
inline void with_any<ChunkOffset>(jsonVal& data, const std::string& key, const ChunkOffset& val) {
  data.WithInteger(key, val);
}

template <>
inline void with_any<int>(jsonVal& data, const std::string& key, const int& val) {
  data.WithInteger(key, val);
}

template <>
inline void with_any<std::string>(jsonVal& data, const std::string& key, const std::string& val) {
  data.WithString(key, val);
}

template <>
inline void with_any<double>(jsonVal& data, const std::string& key, const double& val) {
  data.WithDouble(key, val);
}

template <>
inline void with_any<std::variant<int, double, std::string>>(jsonVal& data, const std::string& key,
                                                             const std::variant<int, double, std::string>& val) {
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
inline void with_any(jsonVal& data, const std::string& key, const T& val) {
  if constexpr (std::is_pointer<T>::value) {
    if (val == nullptr) {
      data.WithString(key, "NULL");
    } else {
      if constexpr (has_properties<std::remove_pointer_t<T>>::value) {
        // nested (T::properties present)
        data.WithObject(key, to_json(val));
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
  } else {
    if constexpr (has_properties<T>::value) {
      // nested (T::properties present)
      data.WithObject(key, to_json(val));
    } else {
      with_any(data, key, val);
    }
  }
}

template <typename T>
jsonVal vec_to_json(const std::vector<T>& vec) {
  jsonVal jv;
  for (size_t idx = 0; idx < vec.size(); ++idx) {
    with_any<T>(jv, std::to_string(idx), vec.at(idx));
  }
  return jv;
}
}  // namespace details

// unserialize function
template <typename T>
T from_json(const jsonView& data) {
  typedef typename std::remove_cv_t<T> without_cv_t;

  if constexpr (std::is_pointer<without_cv_t>::value) {
    // check if we have nullptr
    if (data.IsString() && data.AsString() == "NULL") return nullptr;

    T object = new std::remove_pointer_t<without_cv_t>;
    // number of properties
    constexpr auto nb_properties = std::tuple_size<decltype(std::remove_pointer_t<without_cv_t>::properties)>::value;
    details::for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      // get the property
      constexpr auto property = std::get<i>(std::remove_pointer_t<without_cv_t>::properties);

      // get the type of the property
      using Type = typename decltype(property)::Type;

      // set the value to the member
      object->*(property.member) = details::as_any<Type>(data, property.name);
    });
    // call copy constructor to enable inheritance
    T new_object{object};

    return new_object;
  } else if constexpr (is_smart_ptr<without_cv_t>::value) {
    StaticAssert<!is_weak_ptr<without_cv_t>::value>::stat_assert(
        "Weak pointers are currently not supported by this json serializer");
    StaticAssert<!is_unique_ptr<without_cv_t>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");
    typedef T smart_ptr_t;  // type of the smart pointer
    smart_ptr_t s_ptr_obj;
    typedef decltype(get_inner_t(s_ptr_obj)) inner_t;  // type of the object the pointer is pointing to
    inner_t* object = new inner_t;
    // number of properties
    constexpr auto nb_properties = std::tuple_size<decltype(inner_t::properties)>::value;
    details::for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      // get the property
      constexpr auto property = std::get<i>(inner_t::properties);

      // get the type of the property
      using Type = typename decltype(property)::Type;

      // set the value to the member
      object->*(property.member) = details::as_any<Type>(data, property.name);
    });
    // call copy constructor to enable inheritance
    smart_ptr_t sp = smart_ptr_t(object);
    return sp;
  } else {
    T object;
    constexpr auto nb_properties = std::tuple_size<decltype(T::properties)>::value;
    details::for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      constexpr auto property = std::get<i>(T::properties);
      using Type = typename decltype(property)::Type;
      object.*(property.member) = details::as_any<Type>(data, property.name);
    });
    // call copy constructor to enable inheritance
    T new_object{object};
    return new_object;
  }
}

template <typename T>
jsonVal to_json(const T& object) {
  jsonVal data;
  typedef typename std::remove_reference<std::remove_cv_t<T>> without_cv_t;

  if constexpr (std::is_pointer<without_cv_t>::value) {
    return to_json<std::remove_pointer_t<without_cv_t>>(*object);

  } else if constexpr (is_smart_ptr<without_cv_t>::value) {
    StaticAssert<!is_weak_ptr<without_cv_t>::value>::stat_assert(
        "Weak pointers are currently not supported by this json serializer");
    StaticAssert<!is_unique_ptr<without_cv_t>::value>::stat_assert(
        "Unique pointers are currently not supported by this json serializer");
    without_cv_t s_ptr_obj;
    typedef decltype(get_inner_t(s_ptr_obj)) inner_t;  // type of the object the pointer is pointing to
    return to_json<std::remove_reference<std::remove_cv_t<inner_t>>::value>(*object.get());

  } else if constexpr (has_properties<T>::value) {
    // serialize a class that provides properties tuple
    constexpr auto nb_properties = std::tuple_size<decltype(without_cv_t::properties)>::value;
    details::for_sequence(std::make_index_sequence<nb_properties>{}, [&](auto i) {
      constexpr auto property = std::get<i>(without_cv_t::properties);
      details::with_any(data, property.name, object.*(property.member));
    });

    return data;
  } else if constexpr (has_type_property<T>::value) {
    Fail("hi there");
  } else {
    Fail("unsupported type");
  }
}

template <typename T>
std::string to_json_str(const T& object) {
  return to_json(object).View().WriteReadable();
}
}  // namespace opossum