#pragma once

#include <map>

#include "common.hpp"
#include "types.hpp"
#include "chunk.hpp"
#include "pos_list.hpp"

namespace opossum {

class pos_list_t;

class table {
public:
	table(const size_t chunk_size = 0);
	table(table const&) = delete;
	table(table&&) = default;

	size_t col_count() const;
	size_t row_count() const;
	size_t chunk_count() const;
	chunk& get_chunk(chunk_id_t chunk_id);
	const std::string& get_column_name(size_t column_id) const;
	const std::string& get_column_type(size_t column_id) const;
	size_t get_column_id_by_name(const std::string &column_name) const;

	void add_column(const std::string &name, const std::string &type, bool as_value_column = true);
	void append(std::initializer_list<all_type_variant> values) DEV_ONLY;
	std::vector<int> column_string_widths(int max = 0) const;
	void print(std::ostream &out = std::cout) const;
	pos_list_t get_positions() const;

protected:
	const size_t _chunk_size;
	std::vector<chunk> _chunks;
	std::vector<std::string> _column_names;
	std::vector<std::string> _column_types;
};

}