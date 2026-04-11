#pragma once

#include <memory>

#include "SWIX/src/Swix.hpp"   // → SWseg.hpp → helper.hpp → config.hpp (#define TUNE, globals)
#include "../src/benchmark.h"

// SWIX is a sliding-window temporal learned index.  It stores (key, timestamp)
// pairs and answers queries within the window [ts - TIME_WINDOW, ts].  There is
// no explicit deletion: old records expire automatically once their timestamp
// falls outside the current window.
//
// Mapping to the benchmark framework:
//   bulk_load  — build SWIX from initial keys, assigning timestamps 0..N-1.
//                ts_counter_ is set to TIME_WINDOW afterwards so that each
//                subsequent insert causes exactly one old key to expire
//                (1-for-1 sliding window).
//   insert     — calls swix.insert({key, ts_counter_++}).
//   lower_bound— calls swix.lookup({key, ts_counter_}) and returns the count
//                (no correctness validation; DoShiftingCoreLoop uses a volatile
//                dummy for this slot).
//   erase      — no-op; expiry is handled implicitly by timestamp advancement.
//
// Only the SHIFTING workload is meaningful for SWIX.

namespace deli_testbed {

template <typename KEY_TYPE, typename PAYLOAD_TYPE, size_t WindowSize>
class BenchmarkSWIX : public EmptyBase
{
  public:
    using KeyType     = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;
    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

  private:
    // SWmeta has no default constructor and no move constructor (raw pointer members);
    // heap-allocate and own via unique_ptr, constructed in bulk_load.
    std::unique_ptr<swix::SWmeta<KeyType, KeyType, WindowSize>> index_;
    KeyType ts_counter_ = 0;

  public:
    BenchmarkSWIX() = default;

    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
        const size_t n = static_cast<size_t>(std::distance(begin, end));
        if (n != WindowSize) [[unlikely]] {
            throw std::runtime_error("SWIX: bulk_load size (" + std::to_string(n) +
                                     ") != WindowSize (" + std::to_string(WindowSize) + ")");
        }
        std::vector<std::pair<KeyType, KeyType>> swix_data;
        swix_data.reserve(n);
        KeyType ts = 0;
        for (auto it = begin; it != end; ++it)
            swix_data.emplace_back(it->first, ts++);
        index_ = std::make_unique<swix::SWmeta<KeyType, KeyType, WindowSize>>(swix_data);
        // After insert #k (ts = WindowSize + k) the lowerLimit becomes k,
        // expiring the key whose bulk-load timestamp was k-1.  Perfect 1-for-1.
        ts_counter_ = static_cast<KeyType>(WindowSize);
    }

    PayloadType lower_bound(const KeyType key) {
        auto p = std::make_pair(key, ts_counter_);
        KeyType count = 0;
        index_->lookup(p, count);
        return static_cast<PayloadType>(count);
    }

    void insert(const KeyType& key, const PayloadType& /*payload*/) {
        auto p = std::make_pair(key, ts_counter_++);
        index_->insert(p);
    }

    void erase(const KeyType& /*key*/) {
        // No-op: SWIX uses implicit timestamp-based expiry.
    }

    static std::string name() { return "SWIX"; }
    static std::string variant() { return "none"; }

    bool applicable(const std::string& /*workload*/) { return true; }
};

// ── Benchmark driver ──────────────────────────────────────────────────────────

// Only SHIFTING is registered — the other workloads require KV or PS semantics
// that SWIX does not provide.
//
// The window size is inferred from key_pairs.size() at runtime and must match
// one of the explicitly instantiated WindowSize values below.
template <typename KeyType, typename PayloadType>
void benchmark_swix(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_pairs,
                    const std::vector<std::pair<KeyType, PayloadType>>& shifting_key_pairs) {
    const size_t window = key_pairs.size();

    
    if (window == (1 << 7)) {
        deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 7)>>(
            config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 8)) {
        deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 8)>>(
            config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 9)) {
        deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 9)>>(
            config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 10)) {
        deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 10)>>(
            config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 11)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 11)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 12)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 12)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 13)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 13)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 14)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 14)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 15)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 15)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 16)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 16)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 17)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 17)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 18)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 18)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 19)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 19)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 20)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 20)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 21)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 21)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 22)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 22)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 23)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 23)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 24)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 24)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 25)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 25)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 26)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 26)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 27)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 27)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 28)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 28)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 29)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 29)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else if (window == (1 << 30)) {
    deli_testbed::run_benchmark<BenchmarkSWIX<KeyType, PayloadType, (1 << 30)>>(
        config, key_pairs, Workload::SHIFTING, shifting_key_pairs);
    } else {
        throw std::runtime_error("SWIX benchmark: unsupported window size = number of initial keys = " + std::to_string(window));
    }
}

}  // namespace deli_testbed
