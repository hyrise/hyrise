#include "table.hpp"

namespace opossum {

table::table(const size_t chunk_size) : _chunk_size(chunk_size) {
	_chunks.push_back(chunk());
}

void table::add_column(std::string &&name, column_type type) {
	_column_names.push_back(name);
	_column_types.push_back(type);
	for(auto &chunk : _chunks) {
		chunk.add_column(type);
	}
}

void table::append(std::initializer_list<all_type_variant> values) {
	if(_chunk_size > 0 && _chunks.back().size() == _chunk_size) {
		_chunks.emplace_back();

		// TODO add columns to new chunk
	}

	_chunks.back().append(values);
}

size_t table::col_count() const {
	return _column_types.size();
}

std::vector<int> table::column_string_widths(int max) const {
	std::vector<int> widths(col_count());
	for(auto &&chunk : _chunks) {
		auto widths2 = chunk.column_string_widths(max);
		for(size_t col = 0; col < col_count(); ++col) {
			widths[col] = std::max(widths[col], widths2[col]);
		}
	}
	return widths;
}

void table::print(std::ostream &out) const {
	size_t chunk_id = 0;
	auto widths = column_string_widths(20);
	for(auto &&chunk : _chunks) {
		std::cout << "=== chunk " << chunk_id << " ===" << std::endl;
		chunk.print(out, widths);
	}
}

}