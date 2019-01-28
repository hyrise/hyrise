#pragma once

#include <boost/mpl/aux_/na.hpp>
#include <memory>
#include <optional>

namespace boost {
namespace bimaps {
template <typename L, typename R, typename AP1, typename AP2, typename AP3>
class bimap;
}
}  // namespace boost

namespace opossum {

// This is a wrapper around boost::bimap. We use it because including boost::bimap is really expensive and because the
// bimap cannot be forward-declared properly.
template <typename L, typename R>
class Bimap {
 public:
  // Creates a bimap with the given values
  Bimap(std::initializer_list<std::pair<L, R>> list);

  // Adds a value to the bimap
  void insert(std::pair<L, R>&& pair);

  // For a given left/right value, returns the right/left value. Fails if the entry does not exist.
  const R& left_at(const L& left) const;
  const L& right_at(const R& right) const;

  // For a given left/right value, returns the right/left value or std::nullopt if it does not exist/
  std::optional<R> left_has(const L& right) const;
  std::optional<L> right_has(const R& left) const;

  // Returns all values on the right side as a comma-separated string
  std::string right_as_string() const;

 private:
  // Using shared_ptr for type erasure
  std::shared_ptr<boost::bimaps::bimap<L, R, boost::mpl::na, boost::mpl::na, boost::mpl::na>> _bimap;
};

}  // namespace opossum
