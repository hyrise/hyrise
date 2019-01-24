#include "bimap.hpp"

#include <boost/algorithm/string/join.hpp>
#include <boost/bimap.hpp>
#include <boost/range/adaptors.hpp>

#include "expression/aggregate_expression.hpp"
#include "expression/function_expression.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"

namespace opossum {

/*
 * boost::bimap does not support initializer_lists.
 * Instead we use this helper function to have an initializer_list-friendly interface.
 */
template <typename L, typename R>
Bimap<L, R>::Bimap(std::initializer_list<std::pair<L, R>> list) : _bimap(std::make_shared<boost::bimap<L, R>>()) {}

template <typename L, typename R>
void Bimap<L, R>::insert(std::pair<L, R>&& pair) {
  _bimap->left.insert(std::forward<std::pair<L, R>>(pair));
}

template <typename L, typename R>
const R& Bimap<L, R>::left_at(const L& right) const {
  return _bimap->left.at(right);
}

template <typename L, typename R>
std::optional<R> Bimap<L, R>::left_has(const L& right) const {
  auto it = _bimap->left.find(right);
  if (it == _bimap->left.end()) return std::nullopt;
  return it->second;
}

template <typename L, typename R>
const L& Bimap<L, R>::right_at(const R& left) const {
  return _bimap->right.at(left);
}

template <typename L, typename R>
std::optional<L> Bimap<L, R>::right_has(const R& left) const {
  auto it = _bimap->right.find(left);
  if (it == _bimap->right.end()) return std::nullopt;
  return it->second;
}

template <typename L, typename R>
std::string Bimap<L, R>::right_as_string() const {
  const auto get_first = boost::adaptors::transformed([](auto it) { return it.first; });
  return boost::algorithm::join(_bimap->right | get_first, ", ");
}

template class Bimap<PredicateCondition, std::string>;
template class Bimap<AggregateFunction, std::string>;
template class Bimap<FunctionType, std::string>;
template class Bimap<DataType, std::string>;
template class Bimap<EncodingType, std::string>;
template class Bimap<VectorCompressionType, std::string>;
template class Bimap<TableType, std::string>;

}  // namespace opossum
