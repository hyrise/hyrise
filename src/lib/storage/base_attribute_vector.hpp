#pragma once

#include "common.hpp"
#include "types.hpp"

namespace opossum {

class base_attribute_vector {
public:
	base_attribute_vector() {}
	base_attribute_vector(base_attribute_vector const&) = delete;
	base_attribute_vector(base_attribute_vector&&) = default;

	virtual all_type_variant operator[](const size_t i) const = 0;

	virtual void append(const all_type_variant &val) DEV_ONLY = 0;
	virtual void print(const long row) DEV_ONLY const = 0;
	virtual size_t size() const = 0;
};

}