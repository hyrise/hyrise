#include "hash_function.hpp"

#include "type_cast.hpp"

namespace opossum {

class hash_value_visitor : public boost::static_visitor<HashValue> {
 public:
  
  HashValue operator()(NullValue) { return HashValue{0}; }

  template <typename T>
  HashValue operator()(T value) { return HashValue{std::hash<T>{}(value)}; }
};

const HashValue HashFunction::operator()(const AllTypeVariant value) const {
  hash_value_visitor visitor;
  return boost::apply_visitor(visitor, value);
}

}  // namespace opossum
