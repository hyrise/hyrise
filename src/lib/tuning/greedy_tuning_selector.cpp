#include "greedy_tuning_selector.hpp"

#include <algorithm>
#include <limits>
#include <list>

#include "utils/assert.hpp"

namespace opossum {

namespace {
/*
 * This subroutine scans the least desirable available choices until the
 * accumulated reject_cost() is lower than a required value.
 * If the thereby accumulated reject_desirability is not lower than a threshold
 * an iterator to the choice one past the subsequence to reject is returned.
 * Otherwise the end() iterator is returned, that the constraints are not satisfied
 * by any continuous subsequence starting at sorted_choices.begin().
 */
std::list<std::shared_ptr<TuningOption>>::const_iterator determine_choices_to_sacrifice(
    const std::list<std::shared_ptr<TuningOption>>& sorted_choices, float required_cost_delta,
    float acceptible_desirability_delta = -std::numeric_limits<float>::infinity()) {
  auto desirability_delta = 0.0f;
  auto cost_delta = 0.0f;

  auto choice = sorted_choices.cbegin();
  while (choice != sorted_choices.cend()) {
    if (cost_delta <= required_cost_delta) {
      break;
    }
    desirability_delta += (*choice)->reject_desirability();
    cost_delta += (*choice)->reject_cost();
    ++choice;
  }

  if (cost_delta <= required_cost_delta && desirability_delta >= acceptible_desirability_delta) {
    return choice;
  }
  return sorted_choices.cend();
}

}  // anonymous namespace

std::vector<std::shared_ptr<TuningOperation>> GreedyTuningSelector::select(
    const std::vector<std::shared_ptr<TuningOption>>& choices, float cost_budget) {
  std::vector<std::shared_ptr<TuningOperation>> operations;
  operations.reserve(choices.size());

  // Accumulate absolute cost balance from currently chosen choices.
  float cost_balance = 0.0f;
  for (const auto& choice : choices) {
    // Assumption: cost() >= 0 ==> accept_cost() >= 0 && current_cost() >= 0 && reject_cost() <= 0
    DebugAssert(choice->cost() >= 0, "GreedyTuningSelector cannot deal with negative cost");
    DebugAssert(choice->accept_cost() >= 0, "If cost is >=0, accept_cost must also be >=0");
    DebugAssert(choice->current_cost() >= 0, "If cost is >=, current_cost must also be >= 0");
    DebugAssert(choice->reject_cost() <= 0, "If cost is >=0, reject_cost must be <= 0");

    cost_balance += choice->current_cost();
  }

  // Desirability balance is always relative to current system state.
  float desirability_balance = 0.0f;

  // Sort choices:
  //  1) by reject_cost() ascending
  //  2) by accept_desirability() ascending stable
  // The most desirable choice is at back() and among equaly desirable choices
  // the least costly choices to accept are nearer back().
  std::list<std::shared_ptr<TuningOption>> sorted_choices(choices.cbegin(), choices.cend());

  sorted_choices.sort([](std::shared_ptr<TuningOption> lhs, std::shared_ptr<TuningOption> rhs) {
    return (lhs->accept_desirability() == rhs->accept_desirability())
               ? lhs->reject_cost() < rhs->reject_cost()
               : lhs->accept_desirability() < rhs->accept_desirability();
  });

  // If current state exceeds cost_budget,
  // reject the least desirable choices to reduce cost_balance.
  if (cost_balance > cost_budget) {
    const auto sacrifice_until = determine_choices_to_sacrifice(sorted_choices, cost_budget - cost_balance);
    if (sacrifice_until == sorted_choices.cend()) {
      // Cost budget is impossible to maintain
      return operations;
    }
    for (auto choice = sorted_choices.cbegin(); choice != sacrifice_until; ++choice) {
      // Reject this existing choice to reduce cost balance
      operations.push_back((*choice)->reject());
      cost_balance += (*choice)->reject_cost();
      desirability_balance += (*choice)->reject_desirability();
    }
    sorted_choices.erase(sorted_choices.cbegin(), sacrifice_until);
  }

  // Select the most desirable operations first
  // always maintaining the cost budget
  while (sorted_choices.size() > 0) {
    auto best_choice = sorted_choices.front();
    if (best_choice->reject_desirability() > sorted_choices.back()->accept_desirability()) {
      /*
       * Rejecting a choice can only reduce cost_balance (i.e. free up resources)
       * and never exceed cost_budget (no new costs will be added) as cost() >= 0:
       * Rejecting a choice that already exists yields ("gives back") the cost
       * that was paid for it.
       * Rejecting a choice that does not yet exist cannot cost anything since
       * nothing will be changed.
       */

      operations.push_back(best_choice->reject());
      cost_balance += best_choice->reject_cost();
      desirability_balance += best_choice->reject_desirability();
      sorted_choices.pop_front();

    } else {
      /*
       * Accepting a choice could exceed cost_budget (cost more resources than
       * there are available), so try to reduce cost_balance first (free up used resources)
       */

      const auto sacrifice_until = determine_choices_to_sacrifice(
          sorted_choices, cost_budget - cost_balance - sorted_choices.back()->accept_cost(),
          -sorted_choices.back()->accept_desirability());
      if (sacrifice_until == sorted_choices.cend()) {
        // Reject this choice as required cost would sacrifice more desirability
        operations.push_back(sorted_choices.back()->reject());
        cost_balance += sorted_choices.back()->reject_cost();
        desirability_balance += sorted_choices.back()->reject_desirability();
      } else {
        for (auto choice = sorted_choices.cbegin(); choice != sacrifice_until; ++choice) {
          // Reject this choice in order to reduce cost balance
          operations.push_back((*choice)->reject());
          cost_balance += (*choice)->reject_cost();
          desirability_balance += (*choice)->reject_desirability();
        }
        sorted_choices.erase(sorted_choices.cbegin(), sacrifice_until);

        operations.push_back(sorted_choices.back()->accept());
        cost_balance += sorted_choices.back()->accept_cost();
        desirability_balance += sorted_choices.back()->accept_desirability();

        for (auto choice = sorted_choices.cbegin(); choice != sorted_choices.cend(); ++choice) {
          // Assumption: choice.invalidates() never contains choice itself!
          for (auto invalidated_choice : sorted_choices.back()->invalidates()) {
            auto invalidated_choice_shared_ptr = invalidated_choice.lock();
            DebugAssert(invalidated_choice_shared_ptr, "invalidated choice was deleted");
            if (invalidated_choice_shared_ptr == *choice) {
              operations.push_back((*choice)->reject());
              cost_balance += (*choice)->reject_cost();
              // reject_desirability() of invalid choice is always 0.0f
            }
          }
        }
      }
      sorted_choices.pop_back();
    }
  }

  return operations;
}

}  // namespace opossum
