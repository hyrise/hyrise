#include <memory>

#include "operators/abstract_operator.hpp"
#include "operators/raw_table_scan.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"

int main() {
	auto t = std::make_shared<opossum::table>(opossum::table(2));

	t->add_column("a", "int");
	t->add_column("langer spaltenname", "float");
	t->add_column("c", "string");
	t->add_column("d", "double");

	t->append({123, 456.7, "testa", 51});
	t->append({1234, 457.7, "testb", 516.2});
	t->append({12345, 458.7, "testc", 62});

	auto raw_table_scan = opossum::create_raw_table_scan(t, 0, 12345);
	auto res = raw_table_scan->execute(t->get_positions(), 0);
	res.print();

	std::cout << "!!!!!!!!!!!!!!!!!!" << std::endl;

	auto raw_table_scan2 = opossum::create_raw_table_scan(t, 3, 63);
	auto res2 = raw_table_scan2->execute(res, 3);
	res2.print();

	std::cout << "-----------------" << std::endl;
	std::cout << "-----------------" << std::endl;
	std::cout << "-----------------" << std::endl;

	opossum::storage_manager s;

	s.add_table("tab", std::move(t));

	s.print();
}