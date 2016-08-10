#include <iomanip>
#include <numeric>

#include "table.hpp"

namespace opossum {

table::table(const size_t chunk_size) : _chunk_size(chunk_size) {
	// TODO Sicherstellen, dass chunk_size Zweierpotenz ist
	_chunks.push_back(chunk());
}

void table::add_column(const std::string &name, const std::string &type, bool as_value_column) {
	_column_names.push_back(name);
	_column_types.push_back(type);
	if(as_value_column) {
		for(auto &chunk : _chunks) {
			chunk.add_column(type);
		}
	}
	// TODO default values for existing rows?
}

void table::append(std::initializer_list<all_type_variant> values) {
	// TODO Chunks should be preallocated for chunk size
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

size_t table::get_column_id_by_name(const std::string &column_name) const {
	for(size_t column_id = 0; column_id < col_count(); ++column_id) {
		// TODO make more efficient
		if(_column_names[column_id] == column_name) {
			return column_id;
		}
	}
	throw std::runtime_error("column " + column_name + " not found");
}

const std::string& table::get_column_name(size_t column_id) const {
	return _column_names[column_id];
}

const std::string& table::get_column_type(size_t column_id) const {
	return _column_types[column_id];
}

chunk& table::get_chunk(chunk_id_t chunk_id) {
	return _chunks[chunk_id];
}

// TODO this shouldn't be here, but somewhere else. It could also be more efficient
pos_list_t table::get_positions() const {
	auto r = pos_list_t(this);
	for (chunk_id_t chunk_id = 0; chunk_id < chunk_count(); ++chunk_id) {
		std::vector<chunk_row_id_t> positions(_chunks[chunk_id].size());
		std::iota(positions.begin(), positions.end(), 0);
		r.add_chunk(chunk_id, std::move(positions));
	}

	return r;
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