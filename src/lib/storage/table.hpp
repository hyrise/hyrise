#pragma once

#include "common.hpp"
#include "types.hpp"
#include "chunk.hpp"

namespace opossum {

class table {
public:
	table(const size_t chunk_size = 0);
	table(table const&) = delete;
	table(table&&) = default;

	size_t col_count() const;
	size_t row_count() const;
	size_t chunk_count() const;

	void add_column(std::string &&name, column_type type);
	void append(std::initializer_list<all_type_variant> values) DEV_ONLY;
	void print(std::ostream &out = std::cout) const;

protected:
	const size_t _chunk_size;
	std::vector<chunk> _chunks;
	std::vector<std::string> _column_names;
	std::vector<column_type> _column_types;
};

}