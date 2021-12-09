#include "validation_state.hpp"

#include "dependency_mining_config.hpp"
#include "utils/assert.hpp"

namespace opossum {

ValidationState::ValidationState(const int64_t init_max_validations, const opossum::Duration init_max_duration)
    : max_validations(init_max_validations), max_duration(init_max_duration) {
  const auto default_config = DependencyMiningConfig();
  use_count = init_max_validations > default_config.max_validation_candidates;
  use_time = init_max_duration > default_config.max_validation_time;
  allow_all = !(use_count || use_time);
}

bool ValidationState::keep_running() {
  switch (state) {
    case State::NotStarted:
      validation_begin = std::chrono::high_resolution_clock::now();
      state = State::Running;
      break;
    case State::Over:
      return false;
    default: {
    }
  }

  if (allow_all) return true;

  if (use_count) {
    const auto current_count = validation_count.fetch_add(1);
    if (current_count >= max_validations) {
      set_done();
      return false;
    }
  }

  if (use_time) {
    const auto validation_duration = std::chrono::high_resolution_clock::now() - validation_begin;
    // Stop execution if we reached the time limit
    if (validation_duration >= max_duration) {
      set_done();
      return false;
    }
  }

  return true;
}

void ValidationState::set_done() { state = State::Over; }

bool ValidationState::is_done() const { return state == State::Over; }

bool ValidationState::time_left() const {
  if (allow_all || !use_time) return true;
  return std::chrono::high_resolution_clock::now() - validation_begin < max_duration;
}

}  // namespace opossum
