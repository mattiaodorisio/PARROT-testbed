#pragma once

#include "PGM-index/include/pgm/pgm_index.hpp"

// Wrapper object

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class BenchmarkPGM {
  public:
    BenchmarkPGM() : index() {}
  
    void bulk_load(std::pair<KEY_TYPE, PAYLOAD_TYPE>* values, size_t num_keys) {
      std::sort(values, values + num_keys,
                [](auto const& a, auto const& b) { return a.first < b.first; });

      auto keys = std::ranges::subrange(values, values + num_keys) | std::ranges::views::transform([](auto const& p) { return p.first; });

      // Retain a copy of the data
      data.assign(keys.begin(), keys.end());
      index = pgm::PGMIndex<KEY_TYPE, 32>(data.begin(), data.end());
    }

    PAYLOAD_TYPE lower_bound(const KEY_TYPE& key) {
      auto approx_pos = index.search(key);
      // Last mile search
      auto it = std::lower_bound(data.begin() + approx_pos.lo, data.begin() + approx_pos.hi, key);
      if (it != data.begin() + approx_pos.hi && *it == key) {
        size_t index = std::distance(data.begin(), it);
        return static_cast<PAYLOAD_TYPE>(index); // Return index as payload
      } else {
        return PAYLOAD_TYPE{};
      }
    }

    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      // index.insert(key, payload);
    }

    void erase(const KEY_TYPE& key) {
      // index.erase(key);
    }
  
  private:
    std::vector<KEY_TYPE> data;
    pgm::PGMIndex<KEY_TYPE, 32> index;
};