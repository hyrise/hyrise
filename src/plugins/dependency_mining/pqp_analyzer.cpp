#include "pqp_analyzer.hpp"

#include "candidate_strategy/dependent_group_by_candidate_rule.hpp"
#include "candidate_strategy/join_to_predicate_candidate_rule.hpp"
#include "candidate_strategy/join_to_semi_candidate_rule.hpp"
#include "candidate_strategy/join_elimination_candidate_rule.hpp"
#include "hyrise.hpp"
#include "operators/pqp_utils.hpp"
#include "utils/timer.hpp"

namespace opossum {

PQPAnalyzer::PQPAnalyzer(const std::shared_ptr<DependencyCandidateQueue>& queue) : _queue(queue) {
  if (_enable_groupby_reduction) add_rule(std::make_unique<DependentGroupByCandidateRule>());
  if (_enable_join_to_semi) add_rule(std::make_unique<JoinToSemiCandidateRule>());
  if (_enable_join_to_predicate) add_rule(std::make_unique<JoinToPredicateCandidateRule>());
  if (_enable_join_elimination) add_rule(std::make_unique<JoinEliminationCandidateRule>());
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

    visit_pqp(pqp_root, [&](const auto& op) {
      const auto& lqp_node = op->lqp_node;
      if (!lqp_node) {
        return PQPVisitation::VisitInputs;
      }
      auto prio = static_cast<size_t>(op->performance_data->walltime.count());

      const auto& current_rules = _rules[lqp_node->type];
      for (const auto& rule : current_rules) {
        auto candidates = rule->apply_to_node(lqp_node, prio);
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

  std::cout << "PQPAnalyzer finished in " << timer.lap_formatted() << std::endl;
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
}

}  // namespace opossum
