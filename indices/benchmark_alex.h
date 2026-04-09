#pragma once

#include "ALEX/alex.h"
#include "../src/benchmark.h"

// Wrapper object

namespace deli_testbed {

// BenchmarkALEX supports both key value and predecessor mode:
//
//  - KEY_VALUE (default): ALEX stores (key, payload) pairs and returns the payload
//    of the first entry >= the query key.  Supports all workloads.
//
//  - PREDECESSOR_SEARCH: ALEX is used as an approximate-position index.
//    During bulk_load every `sampling`-th key (with its position in the original
//    sorted array) is inserted into ALEX; This is analogous to what SOSD does.
template <typename KEY_TYPE, typename PAYLOAD_TYPE,
          SearchMode mode = SearchMode::KEY_VALUE,
          size_t sampling = 16>
class BenchmarkALEX
    : public std::conditional_t<mode == SearchMode::PREDECESSOR_SEARCH,
                                PredecessorSearchBase<KEY_TYPE>,
                                EmptyBase>
{
  public:
    using KeyType     = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;
    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

  private:
    using InternalPayload = std::conditional_t<mode == SearchMode::PREDECESSOR_SEARCH,
                                               size_t, PayloadType>;
    alex::Alex<KeyType, InternalPayload> index;

  public:
    BenchmarkALEX() : index() {}

    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH) {
        this->ps_init(begin, end);
        const auto& keys = this->sorted_keys_;

        // Insert every sampling-th key with its position into ALEX.
        std::vector<std::pair<KeyType, size_t>> samples;
        samples.reserve(keys.size() / sampling + 1);
        for (size_t i = 0; i < keys.size(); i += sampling)
          samples.emplace_back(keys[i], i);

        index.bulk_load(samples.data(), static_cast<int>(samples.size()));
      } else {
        index.bulk_load(&*begin, static_cast<int>(std::distance(begin, end)));
      }
    }

    PayloadType lower_bound(const KeyType key) {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH) {
        // ALEX returns the first sampled entry >= key with its stored position.
        auto it = index.lower_bound(key);
        size_t pos;
        if (it.is_end()) {
          // Query is beyond all sampled keys; search near the end.
          pos = this->sorted_keys_.empty() ? 0 : this->sorted_keys_.size() - 1;
        } else {
          pos = static_cast<size_t>(it.payload());
        }
        const size_t lo = (pos >= sampling) ? pos - sampling : 0;
        const size_t hi = pos + 1;  // exclusive; covers the range before this sample
        return static_cast<PayloadType>(this->find_successor_in_range(key, lo, hi));
      } else {
        auto it = index.lower_bound(key);
        return it.is_end() ? PayloadType{} : it.payload();
      }
    }

    void insert(const KeyType& key, const PayloadType& payload)
        requires (mode == SearchMode::KEY_VALUE) {
      index.insert(key, payload);
    }

    void erase(const KeyType& key)
        requires (mode == SearchMode::KEY_VALUE) {
      index.erase(key);
    }

    static std::string name() { return "ALEX"; }

    static std::string variant() {
      if constexpr (mode == SearchMode::PREDECESSOR_SEARCH)
        return "ps-" + std::to_string(sampling);
      return "none";
    }

    bool applicable(const std::string&) { return true; }
};

// ── Benchmark driver functions ────────────────────────────────────────────────

/// KEY_VALUE mode: all dynamic workloads.
template <typename KeyType, typename PayloadType>
void benchmark_alex(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_values,
                    const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {
  constexpr Workload supported_workloads[] = {
      LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, LOOKUP_UNIFORM,
      INSERT_IN_DISTRIBUTION, DELETE_EXISTING, MIXED, SHIFTING};
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkALEX<KeyType, PayloadType, SearchMode::KEY_VALUE>>(
        config, key_values, wl, shifting_insert_key_values);
  }
}

/// PREDECESSOR_SEARCH mode: static lookup workloads only (sampling approach).
/// key_pairs_ps must have key == payload (i.e. the "key_keys" array from main).
template <typename KeyType, typename PayloadType>
void benchmark_alex_ps(const bench_config& config,
                       std::vector<std::pair<KeyType, PayloadType>>& key_pairs_ps,
                       const std::vector<std::pair<KeyType, PayloadType>>& /* shifting unused */) {
  constexpr Workload supported_workloads[] = {LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, LOOKUP_UNIFORM};
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkALEX<KeyType, PayloadType, SearchMode::PREDECESSOR_SEARCH, 16>>(
        config, key_pairs_ps, wl, {});
  }
}

}  // namespace deli_testbed
