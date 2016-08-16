#include "chunk.hpp"
#include "value_column.hpp"
#include "common.hpp"

#include <iomanip>

namespace opossum {

chunk::chunk() {

}

chunk::chunk(std::vector<std::string>& column_types) {
	for (auto &column_type : column_types) {
		add_column(column_type);
	}
}

void chunk::add_column(std::string type) {
	if(DEBUG && _columns.size() > 0 && size() > 0) throw std::runtime_error("Cannot add a column to a non-empty chunk");
	_columns.emplace_back(make_shared_templated<base_column, value_column>(type));
}

void chunk::add_column(std::shared_ptr<base_column> column) {
	_columns.emplace_back(column);
}

void chunk::append(std::initializer_list<all_type_variant> values) {
	if(DEBUG && _columns.size() != values.size()) {
		throw std::runtime_error("append: number of columns (" + to_string(_columns.size()) + ") does not match value list (" + to_string(values.size()) + ")");
	}

	auto column_it = _columns.begin();
	auto value_it = values.begin();
	for(; column_it != _columns.end(); column_it++, value_it++) {
		(*column_it)->append(*value_it);
	}
}

std::vector<int> chunk::column_string_widths(int max) const {
	std::vector<int> widths(_columns.size());
	for(size_t col = 0; col < _columns.size(); ++col) {
		for(size_t row = 0; row < size(); ++row) {
			int width = to_string((*_columns[col])[row]).size();
			if(width > widths[col]) {
				if(width >= max) {
					widths[col] = max;
					break;
				}
				widths[col] = width;
			}
		}
	}
	return widths;
}

std::shared_ptr<base_column> chunk::get_column(size_t column_id) const {
	return _columns[column_id];
}

void chunk::print(std::ostream &out, const std::vector<int> &widths_in) const {
	auto widths = widths_in.size() > 0 ? widths_in : column_string_widths(20);
	for(size_t row = 0; row < size(); ++row) {
		out << "|";
		for(size_t col = 0; col < _columns.size(); ++col) {
			out << std::setw(widths[col]) << (*_columns[col])[row] << "|" << std::setw(0);			
		}
		out << std::endl;
	}
}

size_t chunk::size() const {
	if(DEBUG && _columns.size() == 0) {
		throw std::runtime_error("Can't calculate size on table without columns");
	}
	return _columns.front()->size();
}

}