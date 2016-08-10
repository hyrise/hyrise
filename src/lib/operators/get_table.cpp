#include "get_table.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

get_table::get_table(const std::string &name) : _name(name) {}

const std::string get_table::get_name() const {
	return "get_table";
}

short get_table::get_num_in_tables() const {
	return 0;
}

short get_table::get_num_out_tables() const {
	return 1;
}

void get_table::execute() {
	// no expensive execution to be done here
}

std::shared_ptr<table> get_table::get_output() const {
	return storage_manager::get().get_table(_name);
}

}