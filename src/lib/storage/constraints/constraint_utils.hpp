#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "storage/constraints/table_key_constraint.hpp"

namespace hyrise {

class Table;

/**
 * This file provides helper functions for creating constraints, adding them to tables, and verifying their validity. 
 * Besides improving readability, these functions ensure that only column names from the correct tables are used and
 * that constraints are guaranteed to be valid.
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

/**
* Check if MVCC data tells us that the existing UCC is guaranteed to be still valid. To do this, we can simply check
* if the table chunks have seen any inserts/deletions since the last validation of the UCC. This information is
* contained in the MVCC data of the chunks.
*/
bool constraint_guaranteed_to_be_valid(const std::shared_ptr<Table>& table,
                                       const TableKeyConstraint& table_key_constraint);

}  // namespace hyrise
