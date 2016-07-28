#include "storage/raw_attribute_vector.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace opossum;

class table {
	std::vector<std::shared_ptr<base_attribute_vector>> _columns;

public:
	void add_column(std::shared_ptr<base_attribute_vector> v) {
		_columns.push_back(v);
	}

	std::shared_ptr<base_attribute_vector> get_column(int i) {
		return _columns[i];
	}

	void print(long rows) {
		for(long i = 0; i < rows; ++i) {
			for(size_t j = 0; j < _columns.size(); ++j) {
				_columns[j]->print(i);
				std::cout << "\t\t";
			}
			std::cout << std::endl;
		}
	}

	long get_width() {
		return _columns.size();
	}
};


int main () {
	auto colA = std::make_shared<raw_attribute_vector<int>>();
	auto colB = std::make_shared<raw_attribute_vector<double>>();
	auto colC = std::make_shared<raw_attribute_vector<std::string>>();

	auto t = std::make_shared<table>();
	t->add_column(colA);
	t->add_column(colB);
	t->add_column(colC);

	for(int i = 0; i < t->get_width(); ++i) {
		t->get_column(i)->insert(123);
		t->get_column(i)->insert(123l);
		t->get_column(i)->insert(123.4f);
		t->get_column(i)->insert(123.3);
		t->get_column(i)->insert("123");
		t->get_column(i)->insert("123.3");
	}

	t->print(6);

	std::cout << "search for 123 ..." << std::endl;

	for(int i = 0; i < t->get_width(); ++i) {
		std::cout << "... in column " << i << " of type " << t->get_column(i)->type_name() << std::endl;
		t->get_column(i)->print_all_positions(123);
	}
}