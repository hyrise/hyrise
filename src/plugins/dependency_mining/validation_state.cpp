#include "validation_state.hpp"

#include "utils/assert.hpp"

namespace opossum {

ValidationState::ValidationState(const bool no_limits, const int64_t init_max_validations, const bool use_time_for_validation, const opossum::Duration init_max_duration) : allow_all(no_limits), max_validations(init_max_validations), use_time(use_time_for_validation), max_duration(init_max_duration) {}

// NOLINTNEXTLINE(bugprone-unhandled-self-assignment,cert-oop54-cpp)
ValidationState& ValidationState::operator=(const ValidationState& other) {
  Assert(state != State::Running && other.state != State::Running, "Cannot assign to or from a running validation");
  state = other.state.load();
  validation_count = other.validation_count.load();
  validation_begin = other.validation_begin;
  validation_duration = other.validation_duration;
  max_duration = other.max_duration;
  max_validations = other.max_validations;
  use_time = other.use_time;
  allow_all = other.allow_all;

  return *this;
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

  if (!use_time) {
    const auto current_count = validation_count.fetch_add(1);
    if (current_count >= max_validations) {
      set_done();
      return false;
    }
    return true;
  }

  validation_duration = std::chrono::high_resolution_clock::now() - validation_begin;
  // Stop execution if we reached the time limit
  if (validation_duration >= max_duration) {
    set_done();
    return false;
  }

  return true;
}

void ValidationState::set_done() { state = State::Over; }

bool ValidationState::is_done() const { return state == State::Over; }

bool ValidationState::time_left() const {
  if (!use_time) return true;
  return std::chrono::high_resolution_clock::now() - validation_begin < max_duration;
}

}  // namespace opossum
