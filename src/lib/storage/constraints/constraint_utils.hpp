#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

namespace hyrise {

class Table;

/**
 * This file provides convenience helper functions to create and add constraints to tables. Besides improved
 * readability, these helper functions ensure that only column names from the correct tables are used.
 *
 * Instead of:
 *   customer_table->add_soft_constraint(
 *       TableKeyConstraint{{customer_table->column_id_by_name("c_custkey")}, KeyConstraintType::PRIMARY_KEY});
 *   customer_table->add_soft_constraint(ForeignKeyConstraint{{customer_table->column_id_by_name("c_nationkey")},
 *                                                            customer_table,
 *                                                            {nation_table->column_id_by_name("n_nationkey")},
 *                                                            nation_table});
 * we write:
 *   primary_key_constraint(customer_table, {"c_custkey"});
 *   foreign_key_constraint(customer_table, {"c_nationkey"}, nation_table, {"n_nationkey"});
 */

void primary_key_constraint(const std::shared_ptr<Table>& table, const std::set<std::string>& columns);
void unique_constraint(const std::shared_ptr<Table>& table, const std::set<std::string>& columns);

void foreign_key_constraint(const std::shared_ptr<Table>& foreign_key_table,
                            const std::vector<std::string>& foreign_key_columns,
                            const std::shared_ptr<Table>& primary_key_table,
                            const std::vector<std::string>& primary_key_columns);

void order_constraint(const std::shared_ptr<Table>& table, const std::vector<std::string>& ordering_columns,
                      const std::vector<std::string>& ordered_columns);

}  // namespace hyrise
