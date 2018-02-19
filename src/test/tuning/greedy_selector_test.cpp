#include "../base_test.hpp"

#include "storage/table.hpp"
#include "tuning/greedy_selector.hpp"
#include "tuning/index/index_choice.hpp"
#include "tuning/index/index_operation.hpp"
#include "tuning/null_operation.hpp"
#include "tuning/tuning_choice.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

namespace {

class MockOperation : public TuningOperation {
 public:
  explicit MockOperation(const std::string& name, bool _accepted) : _name{name}, _accepted{_accepted} {}
  const std::string& name() const { return _name; }
  bool accepted() const { return _accepted; }
  void execute() final {}
  void print_on(std::ostream& output) const final { output << "MockOperation(" << _name << ", " << _accepted << ")"; }

 protected:
  std::string _name;
  bool _accepted;
};

class MockChoice : public TuningChoice {
 public:
  explicit MockChoice(const std::string& name, float desirability, float cost, bool exists)
      : _name{name}, _desirability{desirability}, _cost{cost}, _exists{exists} {}

  float desirability() const final { return _desirability; }
  float cost() const final { return _cost; }
  float confidence() const final { return 1.0; }
  bool is_currently_chosen() const final { return _exists; }
  const std::string& name() const { return _name; }

 protected:
  std::shared_ptr<TuningOperation> _accept_operation() const final {
    return std::make_shared<MockOperation>(_name, true);
  }
  std::shared_ptr<TuningOperation> _reject_operation() const final {
    return std::make_shared<MockOperation>(_name, false);
  }

  std::string _name;
  float _desirability;
  float _cost;
  bool _exists;
};

}  // namespace

class GreedySelectorTest : public BaseTest {
 protected:
  void compare_operation(std::shared_ptr<TuningOperation> actual, std::shared_ptr<NullOperation> expected) {
    EXPECT_TRUE(std::dynamic_pointer_cast<NullOperation>(actual));
  }
  void compare_operation(std::shared_ptr<TuningOperation> actual, std::shared_ptr<MockOperation> expected) {
    auto mock_actual = std::dynamic_pointer_cast<MockOperation>(actual);
    EXPECT_TRUE(mock_actual);
    EXPECT_EQ(mock_actual->name(), expected->name());
    EXPECT_EQ(mock_actual->accepted(), expected->accepted());
  }
};

TEST_F(GreedySelectorTest, SelectsBestChoicesInCorrectOrder) {
  GreedySelector selector;
  std::vector<std::shared_ptr<TuningChoice>> choices;

  choices.push_back(std::make_shared<MockChoice>("a", 5.f, 1200.f, false));
  choices.push_back(std::make_shared<MockChoice>("b", 3.f, 500.f, true));
  choices.push_back(std::make_shared<MockChoice>("c", 3.f, 300.f, true));
  choices.push_back(std::make_shared<MockChoice>("d", -8.f, 600.f, true));
  choices.push_back(std::make_shared<MockChoice>("e", 7.f, 800.f, false));
  choices.push_back(std::make_shared<MockChoice>("f", 4.f, 500.f, false));

  auto operations = selector.select(choices, 2000.f);

  EXPECT_EQ(operations.size(), 6u);
  // reject / ignore D
  compare_operation(operations.at(0), std::make_shared<MockOperation>("d", false));
  // accept / create E
  compare_operation(operations.at(1), std::make_shared<MockOperation>("e", true));
  // reject / ignore A
  compare_operation(operations.at(2), std::make_shared<NullOperation>());
  // reject / delete B
  compare_operation(operations.at(3), std::make_shared<MockOperation>("b", false));
  // accept / create F
  compare_operation(operations.at(4), std::make_shared<MockOperation>("f", true));
  // accept / keep C
  compare_operation(operations.at(5), std::make_shared<NullOperation>());
}

}  // namespace opossum
