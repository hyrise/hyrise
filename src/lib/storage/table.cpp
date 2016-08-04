#include "table.hpp"

#include <iomanip>

namespace opossum {

table::table(const size_t chunk_size) : _chunk_size(chunk_size) {
	_chunks.push_back(chunk());
}

void table::add_column(std::string &&name, std::string type) {
	_column_names.push_back(name);
	_column_types.push_back(type);
	for(auto &chunk : _chunks) {
		chunk.add_column(type);
	}
	// TODO default values for existing rows?
}

void table::append(std::initializer_list<all_type_variant> values) {
	if(_chunk_size > 0 && _chunks.back().size() == _chunk_size) {
		_chunks.emplace_back(_column_types);
	}

	_chunks.back().append(values);
}

size_t table::col_count() const {
	return _column_types.size();
}

size_t table::row_count() const {
	size_t ret = 0;
	for (auto &&chunk : _chunks) {
		ret += chunk.size();
	}
	return ret;
}

size_t table::chunk_count() const {
	return _chunks.size();
}

std::vector<int> table::column_string_widths(int max) const {
	std::vector<int> widths(col_count());
	for(size_t col = 0; col < col_count(); ++col) {
		widths[col] = to_string(_column_names[col]).size();
	}
	for(auto &&chunk : _chunks) {
		auto widths2 = chunk.column_string_widths(max);
		for(size_t col = 0; col < col_count(); ++col) {
			widths[col] = std::max(widths[col], widths2[col]);
		}
	}
	return widths;
}

void table::print(std::ostream &out) const {
	auto widths = column_string_widths(20);

	for(size_t col = 0; col < col_count(); ++col) {
		out << "|" << std::setw(widths[col]) << _column_names[col] << std::setw(0);
	}
	out << "|" << std::endl;

	size_t chunk_id = 0;
	for(auto &&chunk : _chunks) {
		out << "=== chunk " << chunk_id << " === " << std::endl;
		chunk_id++;
		chunk.print(out, widths);
	}
}

}