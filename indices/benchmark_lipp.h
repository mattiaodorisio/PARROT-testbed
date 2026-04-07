#pragma once

#include "lipp/src/core/lipp.h"
#include "../src/benchmark.h"

// Wrapper object for LIPP

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkLIPP {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;
    // LIPP only supports exact-match (at()), not lower_bound; KEY_VALUE mode only.
    // So LIPP doesn't support PREDECESSOR_SEARCH mode
    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

    BenchmarkLIPP() : index() {}
  
    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      index.bulk_load(&*begin, std::distance(begin, end));
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      return index.at(key);
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      throw std::runtime_error("Erase not supported on LIPP index");
    }

    static std::string name() {
      return "LIPP";
    }

    static std::string variant() {
      return "none";
    }

    bool applicable(const std::string& data_filename) {
      return true;
    }

  private:
      LIPP<KEY_TYPE, PAYLOAD_TYPE> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_lipp(const bench_config& config,
                    std::vector<std::pair<KeyType, PayloadType>>& key_values,
                    const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {

  // Check if there are duplicates
  for (size_t i = 1; i < key_values.size(); ++i) {
    if (key_values[i].first == key_values[i - 1].first) {
      return;
    }
  }

  constexpr Workload supported_workloads[] = {LOOKUP_EXISTING, INSERT_IN_DISTRIBUTION};

  for (const auto& wl : supported_workloads) {
    run_benchmark<BenchmarkLIPP<KeyType, PayloadType>>(config, key_values, wl, shifting_insert_key_values);
  }
}
}  // namespace deli_testbed
