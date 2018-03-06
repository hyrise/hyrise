#include "hash_function.hpp"

#include <boost/functional/hash.hpp>

#include "type_cast.hpp"

namespace opossum {

class HashValueVisitor : public boost::static_visitor<HashValue> {
 public:
  HashValue operator()(NullValue) { return HashValue{0}; }

  template <typename T>
  HashValue operator()(T value) {
    return HashValue{boost::hash_value(value)};
  }
};

const HashValue HashFunction::operator()(const AllTypeVariant& value) const {
  HashValueVisitor visitor;
  return boost::apply_visitor(visitor, value);
}

HashFunctionType HashFunction::get_type() const { return HashFunctionType::Default; }

}  // namespace opossum
