#include "storage_manager.hpp"

namespace opossum {

void storage_manager::add_table(std::string name, std::shared_ptr<table> tp) {
	_tables.insert(std::make_pair(name, std::move(tp)));
}

void storage_manager::print(std::ostream &out) const {
	out << "===== Tables " << std::endl;

	auto cnt = 0;
	for(auto const &tab : _tables) {
		out << "\t table #" << cnt << ": \t" << tab.first;
		// out << "(" << tab.second->col_count() << "columns, " << tab.second->row_count() << " rows)" << std::endl;
		out << std::endl;
		cnt++;
	}
}

}