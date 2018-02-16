#include "../base_test.hpp"

#include "storage/table.hpp"
#include "tuning/greedy_selector.hpp"
#include "tuning/index/index_choice.hpp"
#include "tuning/index/index_operation.hpp"
#include "tuning/tuning_choice.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

//class MockChoice : public TuningChoice {
//public:
// explicit MockChoice(float desirability, float cost, bool exists)

//     : column_ref{column_ref}, saved_work{0.0f}, exists{exists}, type{ColumnIndexType::Invalid}, memory_cost{0.0f} {}

// float desirability() const final;

// float cost() const final;

// float confidence() const final;

// bool is_currently_chosen() const final;

// const std::set<std::shared_ptr<TuningChoice>>& invalidates() const final;

// void print_on(std::ostream& output) const final;

// /**
//  * The column the this index refers to
//  */
// ColumnRef column_ref;

// /**
//  * An IndexEvaluator specific, signed value that indicates
//  * how this index will affect the overall system performance
//  *
//  * desirability values are relative and only comparable if estimated
//  * by the same IndexEvaluator
//  */
// float saved_work;

// /**
//  * Does this Evaluation refer to an already created index or one that does not exist yet
//  */
// bool exists;

// /**
//  * exists == true: The type of the existing index
//  * exists == false: A proposal for an appropriate index type
//  */
// ColumnIndexType type;

// /**
//  * exists == true: Memory cost in MiB of the index as reported by the index implementation
//  * exists == false: Memory cost in MiB as predicted by the index implementation
//  *                    assuming an equal value distribution across chunks
//  * Measured in MiB
//  */
// float memory_cost;

//protected:
// std::shared_ptr<TuningOperation> _accept_operation() const final;
// std::shared_ptr<TuningOperation> _reject_operation() const final;

// // ToDo(group01) currently unused and empty. Add invalidate logic.
// std::set<std::shared_ptr<TuningChoice>> _invalidates;
//};
//}

class GreedySelectorTest : public BaseTest {

protected:
    void add_index_choice(const std::string& table, ColumnID column_id, bool exists, float saved_work, float memory_cost) {
        auto choice = std::make_shared<IndexChoice>(ColumnRef{table, column_id}, exists);
        choice->saved_work = saved_work;
        choice->memory_cost = memory_cost;
        choices.push_back(choice);
    }

    template<typename T>
    void compare_operations(float budget, const std::vector<std::shared_ptr<T>>& expected_operations) {
        auto operations = selector.select(choices, budget);
        EXPECT_EQ(operations.size(), expected_operations.size());
        for(auto i = 0u; i<expected_operations.size(); i++) {
            auto expected_operation = expected_operations.at(i);
            auto actual_operation = std::dynamic_pointer_cast<T>(operations.at(i));
            EXPECT_TRUE(actual_operation);
            EXPECT_EQ(actual_operation, expected_operation);
        }
    }

    GreedySelector selector;
    std::vector<std::shared_ptr<TuningChoice> > choices;
};

TEST_F(GreedySelectorTest, SelectsBestChoicesInCorrectOrder) {
    auto table = std::make_shared<Table>(1);
    table->add_column("col", DataType::Int);
    StorageManager::get().add_table("a", table);
    StorageManager::get().add_table("b", table);
    StorageManager::get().add_table("c", table);
    StorageManager::get().add_table("d", table);
    StorageManager::get().add_table("e", table);
    StorageManager::get().add_table("f", table);

    add_index_choice("a", ColumnID{0}, false, 5.f, 1200.f);
    add_index_choice("b", ColumnID{0}, true, 3.f, 500.f);
    add_index_choice("c", ColumnID{0}, true, 3.f, 300.f);
    add_index_choice("d", ColumnID{0}, true, -8.f, 600.f);
    add_index_choice("e", ColumnID{0}, false, 7.f, 800.f);
    add_index_choice("f", ColumnID{0}, false, 4.f, 500.f);

    std::vector<std::shared_ptr<IndexOperation>> expected_operations;
    expected_operations.push_back(std::make_shared<IndexOperation>(ColumnRef{"d", ColumnID{0}}, ColumnIndexType::GroupKey, false));
    expected_operations.push_back(std::make_shared<IndexOperation>(ColumnRef{"e", ColumnID{0}}, ColumnIndexType::GroupKey, true));
    expected_operations.push_back(std::make_shared<IndexOperation>(ColumnRef{"a", ColumnID{0}}, ColumnIndexType::GroupKey, false));
    expected_operations.push_back(std::make_shared<IndexOperation>(ColumnRef{"b", ColumnID{0}}, ColumnIndexType::GroupKey, false));
    expected_operations.push_back(std::make_shared<IndexOperation>(ColumnRef{"f", ColumnID{0}}, ColumnIndexType::GroupKey, true));
    expected_operations.push_back(std::make_shared<IndexOperation>(ColumnRef{"c", ColumnID{0}}, ColumnIndexType::GroupKey, true));

    compare_operations<IndexOperation>(2000.f, expected_operations);
}

}  // namespace opossum
