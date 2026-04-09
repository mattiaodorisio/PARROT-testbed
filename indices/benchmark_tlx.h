#pragma once

#include "tlx/tlx/container/btree_set.hpp"
#include "../src/benchmark.h"

#include <algorithm>
#include <vector>

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkTLX {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;
    static constexpr SearchSemantics search_semantics = SearchSemantics::SUCCESSOR;

    BenchmarkTLX() : index() {}

    template <typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      std::vector<KEY_TYPE> keys;
      keys.reserve(std::distance(begin, end));

      for (auto it = begin; it != end; ++it) {
        keys.push_back(it->first);
      }

      keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
      index.bulk_load(keys.begin(), keys.end());
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) {
      auto it = index.lower_bound(key);
      return it != index.end() ? static_cast<PAYLOAD_TYPE>(*it) : PAYLOAD_TYPE{};
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      (void)payload;
      index.insert(key);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }

    static std::string name() {
      return "TLX";
    }

    static std::string variant() {
      return "btree_set";
    }

    bool applicable(const std::string& data_filename) {
      (void)data_filename;
      return true;
    }

  private:
    tlx::btree_set<KEY_TYPE> index;
};

template <typename KeyType, typename PayloadType>
void benchmark_tlx(const bench_config& config,
                   std::vector<std::pair<KeyType, PayloadType>>& key_values,
                   const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {
  constexpr Workload supported_workloads[] = {
      LOOKUP_EXISTING,
      LOOKUP_IN_DISTRIBUTION,
      LOOKUP_UNIFORM,
      INSERT_IN_DISTRIBUTION,
      DELETE_EXISTING,
      MIXED,
      SHIFTING,
  };

  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkTLX<KeyType, PayloadType>>(config, key_values, wl, shifting_insert_key_values);
  }
}
}  // namespace deli_testbed
