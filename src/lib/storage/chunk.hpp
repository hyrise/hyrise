#pragma once

#include "base_attribute_vector.hpp"
#include "raw_attribute_vector.hpp"

namespace opossum {

class chunk {
public:
	chunk();
	chunk(const chunk&) = delete;
	chunk(chunk&&) = default;

	void add_column(column_type type);
	void append(std::initializer_list<all_type_variant> values) DEV_ONLY;
	std::vector<int> column_string_widths(int max = 0) const;
	void print(std::ostream &out = std::cout, const std::vector<int> &column_string_widths = std::vector<int>()) const;
	size_t size() const;

protected:
	std::vector<std::shared_ptr<base_attribute_vector>> _columns;
};

}