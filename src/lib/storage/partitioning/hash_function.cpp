#include "hash_function.hpp"

#include "type_cast.hpp"

namespace opossum {

const HashValue HashFunction::operator()(const AllTypeVariant value) const {
  if (value.type() == typeid(int32_t)) {
    return HashValue{std::hash<int32_t>{}(type_cast<int32_t>(value))};
  }

  if (value.type() == typeid(int64_t)) {
    return HashValue{std::hash<int64_t>{}(type_cast<int64_t>(value))};
  }

  if (value.type() == typeid(float)) {
    return HashValue{std::hash<float>{}(type_cast<float>(value))};
  }

  if (value.type() == typeid(double)) {
    return HashValue{std::hash<double>{}(type_cast<double>(value))};
  }

  if (value.type() == typeid(std::string)) {
    return HashValue{std::hash<std::string>{}(type_cast<std::string>(value))};
  }

  throw "Unknown type, can not hash";
}

}  // namespace opossum
