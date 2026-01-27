#pragma once

#include "PGM-index/include/pgm/pgm_index_dynamic.hpp"
#include "../src/workload.h"

// Wrapper object

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkDynamicPGM {
  public:
    BenchmarkDynamicPGM() : index() {}

    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::sort(values, values + num_keys,
                [](auto const& a, auto const& b) { return a.first < b.first; });

      auto keys = std::ranges::subrange(values, values + num_keys) | std::ranges::views::transform([](auto const& p) { return p.first; });

      // Retain a copy of the data
      data.assign(keys.begin(), keys.end());
      // Re-initialize the index (PGMDynamic does not support copy/move, nor provide a bulk_load function)
      index.~DynamicPGMIndex();
      new (&index) decltype(index)(values, values + num_keys);
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE& key) {
      auto lower_bound_it = index.lower_bound(key);
      return lower_bound_it != index.end() ? lower_bound_it->second : PAYLOAD_TYPE{};
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      index.insert_or_assign(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      index.erase(key);
    }

    static bool is_dynamic() {
      return true;
    }

    static std::string name() {
      return "Dynamic-PGM";
    }

    static std::vector<Workload> supported_workloads() {
      return {LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION, INSERT_IN_DISTRIBUTION};
    }
  
  private:
    std::vector<KEY_TYPE> data;
    pgm::DynamicPGMIndex<KEY_TYPE, PAYLOAD_TYPE> index;
};
