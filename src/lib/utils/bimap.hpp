#pragma once

#include <boost/mpl/aux_/na.hpp>
#include <memory>
#include <optional>

namespace boost {
	namespace bimaps {
		template <typename L, typename R, typename AP1, typename AP2, typename AP3>
		class bimap;
	}
}

namespace opossum {

// TODO Doc
template <typename L, typename R>
class Bimap {
public:
	Bimap(std::initializer_list<std::pair<L, R>> list);
	void insert(std::pair<L, R>&& pair);

	const R& left_at(const L& right) const;
	std::optional<R> left_has(const L& right) const;
	const L& right_at(const R& left) const;
	std::optional<L> right_has(const R& left) const;
	std::string right_as_string() const;

private:
	// Using shared_ptr for type erasure
	std::shared_ptr<boost::bimaps::bimap<L, R, boost::mpl::na, boost::mpl::na ,boost::mpl::na>> _bimap;
};

}  // namespace opossum
