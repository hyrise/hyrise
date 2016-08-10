#include "print.hpp"

namespace opossum {

print::print(const std::shared_ptr<abstract_operator> in) : abstract_operator(in) {}

const std::string print::get_name() const {
	return "print";
}

short print::get_num_in_tables() const {
	return 1;
}

short print::get_num_out_tables() const {
	return 1;
}

void print::execute() {
	// TODO move print method(s) from table/chunk to here
	_input_left->print();
}

std::shared_ptr<table> print::get_output() const {
	return _input_left;
}

}