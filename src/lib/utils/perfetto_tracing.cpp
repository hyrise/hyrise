#include "perfetto_tracing.hpp"

#if HYRISE_WITH_PERFETTO

#include <fcntl.h>
#include <unistd.h>

#include <perfetto.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <iostream>
#include <memory>
#include <string>

#include "utils/assert.hpp"

// Categories are passed as strings at runtime (perfetto::DynamicCategory).
PERFETTO_DEFINE_CATEGORIES(
  perfetto::Category("operator").SetDescription("Execution of physical operators."));
PERFETTO_TRACK_EVENT_STATIC_STORAGE();

namespace {

// No default, must be set. Holds the events of one write period (see below) with a large margin.
constexpr auto BUFFER_SIZE_KB = uint32_t{64 * 1024};

// Default: 5,000 ms. Writing more often keeps the buffer from overflowing between two writes.
constexpr auto FILE_WRITE_PERIOD_MS = uint32_t{500};  // Perfetto also flushes all threads before each write.

// Default: 256 KB, which dropped events under bursts of short operators. 32 MB is the maximum.
constexpr auto SHARED_MEMORY_BUFFER_SIZE_KB = uint32_t{32 * 1024};

// Default: off. Re-emits interned names so that lost events do not make the rest of a thread's trace undecodable.
constexpr auto INCREMENTAL_STATE_CLEAR_PERIOD_MS = uint32_t{1'000};

class TracingSession final {
 public:
  TracingSession() {
    const auto* const path_from_env = std::getenv("HYRISE_PERFETTO_TRACE_FILE");  // NOLINT(concurrency-mt-unsafe)
    _path = path_from_env ? path_from_env : "hyrise.perfetto-trace";

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    _fd = open(_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    Assert(_fd >= 0, std::format("Perfetto: cannot open {} ({}), tracing can't be enabled. "
      "Try setting the HYRISE_PERFETTO_TRACE_FILE environment variable.\n", _path, std::strerror(errno)));

    auto init_args = perfetto::TracingInitArgs{};
    init_args.backends = perfetto::kInProcessBackend;
    init_args.shmem_size_hint_kb = SHARED_MEMORY_BUFFER_SIZE_KB;
    perfetto::Tracing::Initialize(init_args);
    perfetto::TrackEvent::Register();

    auto trace_config = perfetto::TraceConfig{};
    auto* buffer_config = trace_config.add_buffers();
    buffer_config->set_size_kb(BUFFER_SIZE_KB);
    buffer_config->set_fill_policy(perfetto::TraceConfig::BufferConfig::RING_BUFFER);

    trace_config.set_write_into_file(true);
    trace_config.set_file_write_period_ms(FILE_WRITE_PERIOD_MS);
    trace_config.mutable_incremental_state_config()->set_clear_period_ms(INCREMENTAL_STATE_CLEAR_PERIOD_MS);

    auto track_event_config = perfetto::protos::gen::TrackEventConfig{};
    track_event_config.add_enabled_categories("*");
    auto* data_source_config = trace_config.add_data_sources()->mutable_config();
    data_source_config->set_name("track_event");
    data_source_config->set_track_event_config_raw(track_event_config.SerializeAsString());

    _session = perfetto::Tracing::NewTrace();
    _session->Setup(trace_config, _fd);
    _session->StartBlocking();
  }

  TracingSession(const TracingSession&) = delete;
  TracingSession(TracingSession&&) = delete;
  TracingSession& operator=(const TracingSession&) = delete;
  TracingSession& operator=(TracingSession&&) = delete;

  // Called at process exit: flushes pending events, stops the session (which performs the final write), and closes the
  // file. The file descriptor must stay open until StopBlocking() has returned.
  ~TracingSession() {
  if (!_session) {
    return;
  }

  perfetto::TrackEvent::Flush();
  _session->StopBlocking();
  close(_fd);
  std::cerr << "Perfetto trace written to " << _path << " (open at https://ui.perfetto.dev).\n";
  }

 private:
  std::string _path;
  int32_t _fd{-1};
  std::unique_ptr<perfetto::TracingSession> _session;
};

}  // namespace

namespace hyrise {

void ensure_tracing_started() {
  // Static local: thread-safe lazy initialization.
  static auto session = TracingSession{};
}

void trace_event_begin(std::string_view category, std::string_view name) {
  ensure_tracing_started();
  // Perfetto requires the category as a variable and the name as a temporary.
  auto dynamic_category = perfetto::DynamicCategory{std::string{category}};
  TRACE_EVENT_BEGIN(dynamic_category, (perfetto::DynamicString{name.data(), name.size()}));
}

void trace_event_end(std::string_view category, std::string_view description) {
  auto dynamic_category = perfetto::DynamicCategory{std::string{category}};
  if (description.empty()) {
  TRACE_EVENT_END(dynamic_category);
  return;
  }
  TRACE_EVENT_END(dynamic_category, "description", std::string{description});
}

}  // namespace hyrise

#endif
