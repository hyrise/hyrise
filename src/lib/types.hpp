#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>
#include <string>

namespace opossum {

using all_type_variant = boost::variant<int, long, float, double, std::string>;

template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::type
type_cast(all_type_variant value) {
	try {
		return boost::lexical_cast<T>(value);
	} catch(...) {
		return boost::numeric_cast<T>(boost::lexical_cast<double>(value));
	}
}

template<typename T>
typename std::enable_if<std::is_floating_point<T>::value, T>::type
type_cast(all_type_variant value) {
	return boost::lexical_cast<T>(value);
}

template<typename T>
typename std::enable_if<std::is_same<T, std::string>::value, T>::type
type_cast(all_type_variant value) {
	return boost::lexical_cast<T>(value);
}

}