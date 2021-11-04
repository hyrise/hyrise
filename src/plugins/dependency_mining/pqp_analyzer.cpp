#include "pqp_analyzer.hpp"

#include "candidate_strategy/dependent_group_by_candidate_rule.hpp"
#include "candidate_strategy/join_elimination_candidate_rule.hpp"
#include "candidate_strategy/join_to_predicate_candidate_rule.hpp"
#include "candidate_strategy/join_to_semi_candidate_rule.hpp"
#include "hyrise.hpp"
#include "operators/pqp_utils.hpp"
#include "utils/timer.hpp"
#include "gather_required_expressions.hpp"

namespace opossum {

PQPAnalyzer::PQPAnalyzer(const std::shared_ptr<DependencyCandidateQueue>& queue) : _queue(queue) {
  if (ENABLE_GROUPBY_REDUCTION) add_rule(std::make_unique<DependentGroupByCandidateRule>());
  if (ENABLE_JOIN_TO_SEMI) add_rule(std::make_unique<JoinToSemiCandidateRule>());
  if (ENABLE_JOIN_TO_PREDICATE) add_rule(std::make_unique<JoinToPredicateCandidateRule>());
  if (ENABLE_JOIN_ELIMINATION) add_rule(std::make_unique<JoinEliminationCandidateRule>());
}

void PQPAnalyzer::run() {
  const auto& pqp_cache = Hyrise::get().default_pqp_cache;
  if (!pqp_cache) {
    std::cout << "NO PQPCache. Stopping" << std::endl;
    return;
  }
  const auto cache_snapshot = pqp_cache->snapshot();

  if (cache_snapshot.empty()) {
    std::cout << "PQPCache empty. Stopping" << std::endl;
    return;
  }

  std::cout << "Run PQPAnalyzer" << std::endl;
  Timer timer;

  for (const auto& [_, entry] : cache_snapshot) {
    const auto pqp_root = entry.value;
    const auto& lqp_root = pqp_root->lqp_node;
    if (!lqp_root) {
      std::cout << " No LQP root found" << std::endl;
      continue;
    }

    std::unordered_map<std::shared_ptr<const AbstractLQPNode>, ExpressionUnorderedSet> required_expressions_by_node;
    if constexpr (ENABLE_JOIN_TO_SEMI || ENABLE_JOIN_TO_PREDICATE || ENABLE_JOIN_ELIMINATION) {
      // Add top-level columns that need to be included as they are the actual output
      const auto output_expressions = lqp_root->output_expressions();
      required_expressions_by_node[lqp_root].insert(output_expressions.cbegin(), output_expressions.cend());

      std::unordered_map<std::shared_ptr<const AbstractLQPNode>, size_t> outputs_visited_by_node;
      recursively_gather_required_expressions(lqp_root, required_expressions_by_node, outputs_visited_by_node);
    }


    visit_pqp(pqp_root, [&](const auto& op) {
      const auto& lqp_node = op->lqp_node;
      if (!lqp_node) {
        return PQPVisitation::VisitInputs;
      }
      auto prio = static_cast<size_t>(op->performance_data->walltime.count());

      const auto& current_rules = _rules[lqp_node->type];
      for (const auto& rule : current_rules) {
        auto candidates = rule->apply_to_node(lqp_node, prio, required_expressions_by_node);
        for (auto& candidate : candidates) {
          _add_if_new(candidate);
        }
      }
      return PQPVisitation::VisitInputs;
    });
  }

  if (_queue) {
    for (auto& candidate : _known_candidates) {
      _queue->emplace(candidate);
    }
  }

  std::cout << "PQPAnalyzer generated " << _known_candidates.size() << " candidates in " << timer.lap_formatted()
            << std::endl;
}

void PQPAnalyzer::add_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule) {
  _rules[rule->target_node_type].emplace_back(std::move(rule));
}

void PQPAnalyzer::_add_if_new(DependencyCandidate& candidate) {
  for (auto& known_candidate : _known_candidates) {
    if (known_candidate.type != candidate.type) {
      continue;
    }
    if (candidate.dependents == known_candidate.dependents && candidate.determinants == known_candidate.determinants) {
      if (known_candidate.priority < candidate.priority) {
        known_candidate.priority = candidate.priority;
      }
      return;
    }
  }
  _known_candidates.emplace_back(std::move(candidate));
  /*auto it = _known_candidates.find(candidate);
  if (it == _known_candidates.end()) {
    _known_candidates.emplace(std::move(candidate));
    return;
  }
  auto& known_candidate = *it;
  if (known_candidate.priority < candidate.priority) {
    known_candidate.priority = candidate.priority;
  }*/
}

}  // namespace opossum
