#include "../base_test.hpp"

#include "storage/table.hpp"
#include "tuning/greedy_tuning_selector.hpp"
#include "tuning/index/index_tuning_operation.hpp"
#include "tuning/index/index_tuning_option.hpp"
#include "tuning/null_tuning_operation.hpp"
#include "tuning/tuning_operation.hpp"
#include "tuning/tuning_option.hpp"

namespace opossum {

namespace {

class MockTuningOperation : public TuningOperation {
 public:
  explicit MockTuningOperation(const std::string& name, bool _accepted) : _name{name}, _accepted{_accepted} {}
  const std::string& name() const { return _name; }
  bool accepted() const { return _accepted; }
  void execute() final {}
  void print_on(std::ostream& output) const final {
    output << "MockTuningOperation(" << _name << ", " << _accepted << ")";
  }

 protected:
  std::string _name;
  bool _accepted;
};

class MockTuningOption : public TuningOption {
 public:
  explicit MockTuningOption(const std::string& name, float desirability, float cost, bool exists)
      : _name{name}, _desirability{desirability}, _cost{cost}, _exists{exists} {}

  float desirability() const final { return _desirability; }
  float cost() const final { return _cost; }
  float confidence() const final { return 1.0; }
  bool is_currently_chosen() const final { return _exists; }
  const std::string& name() const { return _name; }

 protected:
  std::shared_ptr<TuningOperation> _accept_operation() const final {
    return std::make_shared<MockTuningOperation>(_name, true);
  }
  std::shared_ptr<TuningOperation> _reject_operation() const final {
    return std::make_shared<MockTuningOperation>(_name, false);
  }

  std::string _name;
  float _desirability;
  float _cost;
  bool _exists;
};

}  // namespace

class GreedyTuningSelectorTest : public BaseTest {
 protected:
  void compare_operation(std::shared_ptr<TuningOperation> actual, std::shared_ptr<NullTuningOperation> expected) {
    EXPECT_TRUE(std::dynamic_pointer_cast<NullTuningOperation>(actual));
  }
  void compare_operation(std::shared_ptr<TuningOperation> actual, std::shared_ptr<MockTuningOperation> expected) {
    auto mock_actual = std::dynamic_pointer_cast<MockTuningOperation>(actual);
    EXPECT_TRUE(mock_actual);
    EXPECT_EQ(mock_actual->name(), expected->name());
    EXPECT_EQ(mock_actual->accepted(), expected->accepted());
  }
};

TEST_F(GreedyTuningSelectorTest, SelectsBestTuningOptionsInCorrectOrder) {
  GreedyTuningSelector selector;
  std::vector<std::shared_ptr<TuningOption>> choices;

  choices.push_back(std::make_shared<MockTuningOption>("a", 5.f, 1200.f, false));
  choices.push_back(std::make_shared<MockTuningOption>("b", 3.f, 500.f, true));
  choices.push_back(std::make_shared<MockTuningOption>("c", 3.f, 300.f, true));
  choices.push_back(std::make_shared<MockTuningOption>("d", -8.f, 600.f, true));
  choices.push_back(std::make_shared<MockTuningOption>("e", 7.f, 800.f, false));
  choices.push_back(std::make_shared<MockTuningOption>("f", 4.f, 500.f, false));

  auto operations = selector.select(choices, 2000.f);

  EXPECT_EQ(operations.size(), 6u);
  // reject / ignore D
  compare_operation(operations.at(0), std::make_shared<MockTuningOperation>("d", false));
  // accept / create E
  compare_operation(operations.at(1), std::make_shared<MockTuningOperation>("e", true));
  // reject / ignore A
  compare_operation(operations.at(2), std::make_shared<NullTuningOperation>());
  // reject / delete B
  compare_operation(operations.at(3), std::make_shared<MockTuningOperation>("b", false));
  // accept / create F
  compare_operation(operations.at(4), std::make_shared<MockTuningOperation>("f", true));
  // accept / keep C
  compare_operation(operations.at(5), std::make_shared<NullTuningOperation>());
}

}  // namespace opossum
