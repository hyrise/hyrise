#include "perfetto_tracing.hpp"

#if HYRISE_WITH_PERFETTO

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

PERFETTO_TRACK_EVENT_STATIC_STORAGE();

namespace {

/**
 * Perfetto collects events in an in-memory buffer and writes them to the output file every FILE_WRITE_PERIOD_MS
 * (`write_into_file`). The buffer therefore only has to hold the events of one write period, not the entire trace.
 */
constexpr auto BUFFER_SIZE_KB = uint32_t{64 * 1024};
constexpr auto FILE_WRITE_PERIOD_MS = uint32_t{500};  // Perfetto also flushes all threads before each write.

/**
 * Before reaching the buffer above, events pass through a shared memory buffer (SMB) between the traced threads and
 * Perfetto's service. If threads emit events faster than the service moves them on (e.g., bursts of many short
 * operators), the SMB fills up and Perfetto silently drops events. 
 * We use the largest possible SMB to make this unlikely.
 */
constexpr auto SHARED_MEMORY_BUFFER_SIZE_KB = uint32_t{32 * 1024};

/**
 * Safety net in case events are lost anyway: event names and thread descriptions are written only once per thread and
 * later events refer back to them. If these first packets are lost, the trace processor cannot decode anything that
 * follows for that thread. Re-emitting them periodically limits the damage to at most one period.
 */
constexpr auto INCREMENTAL_STATE_CLEAR_PERIOD_MS = uint32_t{1'000};

class TracingSession final {
 public:
  TracingSession() {
    const auto* const path_from_env = std::getenv("HYRISE_PERFETTO_TRACE_FILE");  // NOLINT(concurrency-mt-unsafe)
    _path = path_from_env ? path_from_env : "hyrise.perfetto-trace";

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    _fd = open(_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (_fd < 0) {
      std::cerr << "Perfetto: cannot open " << _path << " (" << std::strerror(errno)  // NOLINT(concurrency-mt-unsafe)
                << "), tracing disabled.\n";
      return;
    }

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
  int _fd{-1};
  std::unique_ptr<perfetto::TracingSession> _session;
};

}  // namespace

namespace hyrise {

void ensure_tracing_started() {
  // Static local: thread-safe lazy initialization.
  static auto session = TracingSession{};
}

}  // namespace hyrise

#endif
