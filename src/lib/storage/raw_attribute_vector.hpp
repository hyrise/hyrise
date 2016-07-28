#pragma once

#include "base_attribute_vector.hpp"

#include <vector>
#include <iostream>

namespace opossum {

template<typename T>
class raw_attribute_vector : public base_attribute_vector {
public:
	raw_attribute_vector() {std::cout << typeid(T).name() << std::endl;}

	virtual all_type_variant operator[](const size_t i) const {
		return _values[i];
	}	

	virtual void append(const all_type_variant &val) {
		_values.push_back(type_cast<T>(val));
	}

	virtual void print(const long row) const {
		std::cout << _values[row];
	}

	virtual size_t size() const {
		return _values.size();
	}

protected:
	std::vector<T> _values;
};

}