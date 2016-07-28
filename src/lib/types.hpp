#pragma once

#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>
#include <string>

namespace opossum {

enum column_type {int_type, long_type, float_type, double_type, string_type};
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
	// TODO is lexical_cast always necessary?
	return boost::lexical_cast<T>(value);
}

template<typename T>
typename std::enable_if<std::is_same<T, std::string>::value, T>::type
type_cast(all_type_variant value) {
	return boost::lexical_cast<T>(value);
}

std::string to_string(const all_type_variant& x);

}