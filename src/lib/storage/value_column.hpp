#pragma once

#include "base_column.hpp"

#include <vector>
#include <iostream>

namespace opossum {

template<typename T>
class value_column : public base_column {
public:
	value_column() {}

	virtual all_type_variant operator[](const size_t i) const {
		return _values[i];
	}

	virtual void append(const all_type_variant &val) {
		_values.push_back(type_cast<T>(val));
	}

	const std::vector<T>& get_values() const {
		return _values;
	}

	virtual size_t size() const {
		return _values.size();
	}

protected:
	std::vector<T> _values;
};

}