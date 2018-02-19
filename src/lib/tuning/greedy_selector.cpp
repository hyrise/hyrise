#include "greedy_selector.hpp"

#include <algorithm>
#include <limits>
#include <list>

#include "utils/logging.hpp"

namespace opossum {

// This subroutine scans the least desirable available choices until the
// accumulated reject_cost() is lower than a required value.
// If the thereby accumulated reject_desirability is not lower than a threshold
// an iterator to the choice one past the subsequence to reject is returned.
// Otherwise the end() iterator is returned, that the constraints are not satisfied
// by any continuous subsequence starting at sorted_choices.begin().
std::list<std::shared_ptr<TuningChoice>>::iterator sacrifice_choices(
    std::list<std::shared_ptr<TuningChoice>>& sorted_choices, float required_cost_delta,
    float acceptible_desirability_delta = -std::numeric_limits<float>::infinity()) {
  float desirability_delta = 0.0f;
  float cost_delta = 0.0f;

  auto choice = sorted_choices.begin();
  while (choice != sorted_choices.end()) {
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
  return sorted_choices.end();
}

std::vector<std::shared_ptr<TuningOperation>> GreedySelector::select(
    const std::vector<std::shared_ptr<TuningChoice>>& choices, float cost_budget) {
  std::vector<std::shared_ptr<TuningOperation>> operations;
  operations.reserve(choices.size());
  // Assumption: cost() >= 0 ==> accept_cost() >= 0 && current_cost() >= 0 && reject_cost() <= 0

  // Accumulate absolute cost balance from currently chosen choices.
  float cost_balance = 0.0f;
  for (const auto& choice : choices) {
    cost_balance += choice->current_cost();
  }

  // Desirability balance is always relative to current system state.
  float desirability_balance = 0.0f;

  // Sort choices:
  //  1) by reject_cost() ascending
  //  2) by accept_desirability() ascending stable
  // The most desirable choice is at back() and among equaly desirable choices
  // the least costly choices to accept are nearer back().
  std::list<std::shared_ptr<TuningChoice>> sorted_choices(choices.size());
  std::copy(choices.cbegin(), choices.cend(), sorted_choices.begin());
  sorted_choices.sort([](std::shared_ptr<TuningChoice> lhs, std::shared_ptr<TuningChoice> rhs) {
    return lhs->reject_cost() < rhs->reject_cost();
  });
  sorted_choices.sort([](std::shared_ptr<TuningChoice> lhs, std::shared_ptr<TuningChoice> rhs) {
    return lhs->accept_desirability() < rhs->accept_desirability();
  });

  // If current state exceeds cost_budget,
  // reject the least desirable choices to reduce cost_balance.
  if (cost_balance > cost_budget) {
    LOG_INFO("Cost balance of " << cost_balance << " exceeds budget of " << cost_budget);
    auto sacrifice_until = sacrifice_choices(sorted_choices, cost_budget - cost_balance);
    if (sacrifice_until == sorted_choices.end()) {
      LOG_WARN("Cost budget is impossible to maintain. No Operations are performed.");
      return operations;
    }
    for (auto choice = sorted_choices.begin(); choice != sacrifice_until; ++choice) {
      LOG_DEBUG(" ! Reject " << **choice << " to reduce cost balance");
      operations.push_back((*choice)->reject());
      cost_balance += (*choice)->reject_cost();
      desirability_balance += (*choice)->reject_desirability();
    }
    sorted_choices.erase(sorted_choices.begin(), sacrifice_until);
  }

  // Select the most desirable operations first
  // always maintaining the cost budget
  while (sorted_choices.size() > 0) {
    if (sorted_choices.front()->reject_desirability() > sorted_choices.back()->accept_desirability()) {
      LOG_DEBUG("Rejecting " << *sorted_choices.front() << " is most beneficial.");
      // Rejecting a choice can only reduce cost_balance and never exceed cost_budget (as cost()>=0)

      LOG_DEBUG(" ! Reject " << *sorted_choices.front());
      operations.push_back(sorted_choices.front()->reject());
      cost_balance += sorted_choices.front()->reject_cost();
      desirability_balance += sorted_choices.front()->reject_desirability();
      sorted_choices.pop_front();

    } else {
      LOG_DEBUG("Accepting " << *sorted_choices.back() << " is most beneficial.");
      // Accepting a choice could exceed cost_budget, so reduce cost_balance first

      auto sacrifice_until =
          sacrifice_choices(sorted_choices, cost_budget - cost_balance - sorted_choices.back()->accept_cost(),
                            -sorted_choices.back()->accept_desirability());
      if (sacrifice_until == sorted_choices.end()) {
        LOG_DEBUG(" ! Reject " << *sorted_choices.back() << " as required cost would sacrifice more desirability.");
        operations.push_back(sorted_choices.back()->reject());
        cost_balance += sorted_choices.back()->reject_cost();
        desirability_balance += sorted_choices.back()->reject_desirability();
      } else {
        for (auto choice = sorted_choices.begin(); choice != sacrifice_until; ++choice) {
          LOG_DEBUG(" ! Reject " << **choice << " to reduce cost balance");
          operations.push_back((*choice)->reject());
          cost_balance += (*choice)->reject_cost();
          desirability_balance += (*choice)->reject_desirability();
        }
        sorted_choices.erase(sorted_choices.begin(), sacrifice_until);

        LOG_DEBUG(" ! Accept " << *sorted_choices.back());
        operations.push_back(sorted_choices.back()->accept());
        cost_balance += sorted_choices.back()->accept_cost();
        desirability_balance += sorted_choices.back()->accept_desirability();

        for (auto choice = sorted_choices.begin(); choice != sorted_choices.end(); ++choice) {
          // Assumption: choice.invalidates() never contains choice itself!
          if (sorted_choices.back()->invalidates().count(*choice) > 0) {
            LOG_DEBUG(" ! Reject " << **choice << " because it was invalidated");
            operations.push_back((*choice)->reject());
            cost_balance += (*choice)->reject_cost();
            // reject_desirability() of invalid choice is always 0.0f
          }
        }
      }
      sorted_choices.pop_back();
    }
  }

  LOG_INFO("Desirability delta: " << desirability_balance << "; Cost balance: " << cost_balance);
  return operations;
}

}  // namespace opossum
