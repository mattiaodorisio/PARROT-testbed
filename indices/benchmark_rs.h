#pragma once

#include "RadixSpline/include/rs/builder.h"
#include "RadixSpline/include/rs/radix_spline.h"
#include "../src/benchmark.h"
#include "../src/utils.h"

// Wrapper object

namespace deli_testbed {
template <typename KEY_TYPE, typename PAYLOAD_TYPE, int size_scale>
class BenchmarkRS {
  public:
    using KeyType = KEY_TYPE;
    using PayloadType = PAYLOAD_TYPE;

    BenchmarkRS() {}
  
    template<typename Iterator>
    void bulk_load(const Iterator begin, const Iterator end) {
      auto keys_iter = std::ranges::subrange(begin, end) | std::ranges::views::transform([](auto const& p) { return p.first; });

      auto min = std::numeric_limits<KeyType>::min();
      auto max = std::numeric_limits<KeyType>::max();
      if (keys_iter.size() > 0) {
        min = keys_iter.front();
        max = keys_iter.back();
      }
      rs::Builder<KeyType> rsb(min, max, num_radix_bits_, max_error_);
      for (const auto& key : keys_iter) rsb.AddKey(key);
      rs_ = rsb.Finalize();

      // Unlike dynamic indexes (ALEX, LIPP, Dynamic-PGM) RS does not have payloads
      keys.assign(keys_iter.begin(), keys_iter.end());
    }
  
    PAYLOAD_TYPE lower_bound(const KEY_TYPE key) const {
      const rs::SearchBound sb = rs_.GetSearchBound(key);

      auto it = std::lower_bound(keys.begin() + sb.begin, keys.begin() + sb.end, key);
      if (it != keys.begin() + sb.end) {
        size_t index = std::distance(keys.begin(), it);
        return keys[index];
      } else {
        return PAYLOAD_TYPE{};
      }
    }
  
    void insert(const KEY_TYPE& key, const PAYLOAD_TYPE& payload) {
      throw std::runtime_error("Insert not supported on RadixSpline index");
    }

    void erase(const KEY_TYPE& key) {
      throw std::runtime_error("Erase not supported on RadixSpline index");
    }

    static std::string name() {
      return "RadixSpline";
    }

    static std::string variant() {
      return std::to_string(size_scale);
    }

    bool applicable(const std::string& data_filename) {
      // Get the file name from the path.
      std::string dataset = data_filename.substr(data_filename.find_last_of("/\\") + 1);

      // Set parameters based on the dataset.
      return SetParameters(dataset);
    }

  private:

    // Taken from SOSD: https://github.com/learnedsystems/SOSD/blob/master/competitors/rs.h
    bool SetParameters(const std::string& dataset) {
      assert(size_scale >= 1 && size_scale <= 10);

      using Config = std::pair<size_t, size_t>;
      std::vector<Config> configs;

      if (dataset == "normal_200M_uint32") {
        configs = {{10, 6}, {15, 1}, {16, 1}, {18, 1}, {20, 1},
                  {21, 1}, {24, 1}, {25, 1}, {26, 1}, {26, 1}};
      } else if (dataset == "normal_200M_uint64") {
        configs = {{14, 2}, {16, 1}, {16, 1}, {20, 1}, {22, 1},
                  {24, 1}, {26, 1}, {26, 1}, {28, 1}, {28, 1}};
      } else if (dataset == "lognormal_200M_uint32") {
        configs = {{12, 20}, {16, 3}, {16, 2}, {18, 1}, {20, 1},
                  {22, 1},  {24, 1}, {24, 1}, {26, 1}, {28, 1}};
      } else if (dataset == "lognormal_200M_uint64") {
        configs = {{12, 3}, {18, 1}, {18, 1}, {20, 1}, {22, 1},
                  {24, 1}, {26, 1}, {26, 1}, {28, 1}, {28, 1}};
      } else if (dataset == "uniform_dense_200M_uint32") {
        configs = {{4, 2},  {16, 2}, {18, 1}, {20, 1}, {20, 1},
                  {22, 2}, {24, 1}, {26, 3}, {26, 3}, {28, 2}};
      } else if (dataset == "uniform_dense_200M_uint64") {
        configs = {{4, 2},  {16, 1}, {16, 1}, {20, 1}, {22, 1},
                  {24, 1}, {24, 1}, {26, 1}, {28, 1}, {28, 1}};
      } else if (dataset == "uniform_sparse_200M_uint32") {
        configs = {{12, 220}, {14, 100}, {14, 80}, {16, 30}, {18, 20},
                  {20, 10},  {20, 8},   {20, 5},  {24, 3},  {26, 1}};
      } else if (dataset == "uniform_sparse_200M_uint64") {
        configs = {{12, 150}, {14, 70}, {16, 50}, {18, 20}, {20, 20},
                  {20, 9},   {20, 5},  {24, 3},  {26, 2},  {28, 1}};
      } else if (dataset == "books_200M_uint32") {
        configs = {{14, 250}, {14, 250}, {16, 190}, {18, 80}, {18, 50},
                  {22, 20},  {22, 9},   {22, 8},   {24, 3},  {28, 2}};
      } else if (dataset == "books_200M_uint64") {
        configs = {{12, 380}, {16, 170}, {16, 110}, {20, 50}, {20, 30},
                  {22, 20},  {22, 10},  {24, 3},   {26, 3},  {28, 2}};
      } else if (dataset == "books_400M_uint64") {
        configs = {{16, 220}, {16, 220}, {18, 160}, {20, 60}, {20, 40},
                  {22, 20},  {22, 7},   {26, 3},   {28, 2},  {28, 1}};
      } else if (dataset == "books_600M_uint64") {
        configs = {{18, 330}, {18, 330}, {18, 190}, {20, 70}, {22, 50},
                  {22, 20},  {24, 7},   {26, 3},   {28, 2},  {28, 1}};
      } else if (dataset == "books_800M_uint64") {
        configs = {{18, 320}, {18, 320}, {18, 200}, {22, 80}, {22, 60},
                  {22, 20},  {24, 9},   {26, 3},   {28, 3},  {28, 3}};
      } else if (dataset == "fb_200M_uint64") {
        configs = {{8, 140}, {8, 140}, {8, 140}, {8, 140}, {10, 90},
                  {22, 90}, {24, 70}, {26, 80}, {26, 7},  {28, 80}};
      } else if (dataset == "osm_cellids_200M_uint64") {
        configs = {{20, 160}, {20, 160}, {20, 160}, {20, 160}, {20, 80},
                  {24, 40},  {24, 20},  {26, 8},   {26, 3},   {28, 2}};
      } else if (dataset == "osm_cellids_400M_uint64") {
        configs = {{20, 190}, {20, 190}, {20, 190}, {20, 190}, {22, 80},
                  {24, 20},  {26, 20},  {26, 10},  {28, 6},   {28, 2}};
      } else if (dataset == "osm_cellids_600M_uint64") {
        configs = {{20, 190}, {20, 190}, {20, 190}, {22, 180}, {22, 100},
                  {24, 20},  {26, 20},  {28, 7},   {28, 5},   {28, 2}};
      } else if (dataset == "osm_cellids_800M_uint64") {
        configs = {{22, 190}, {22, 190}, {22, 190}, {22, 190}, {24, 190},
                  {26, 30},  {26, 20},  {28, 7},   {28, 5},   {28, 1}};
      } else if (dataset == "wiki_ts_200M_uint64") {
        configs = {{14, 100}, {14, 100}, {16, 60}, {18, 20}, {20, 20},
                  {20, 9},   {20, 5},   {22, 3},  {26, 2},  {26, 1}};
      } else {
        // No config.
        return false;
      }

      const Config config = configs[size_scale - 1];
      num_radix_bits_ = config.first;
      max_error_ = config.second;
      parameters_set_ = true;
      return true;
    }

    std::vector<KeyType> keys;
    rs::RadixSpline<KeyType> rs_;
    size_t num_radix_bits_;
    size_t max_error_;
    bool parameters_set_ = false;
};

template <typename KeyType, typename PayloadType>
void benchmark_rs(const bench_config& config,
                             std::vector<std::pair<KeyType, PayloadType>>& key_values,
                             const std::vector<std::pair<KeyType, PayloadType>>& shifting_insert_key_values) {

  constexpr Workload supported_workloads[] = { LOOKUP_EXISTING, LOOKUP_IN_DISTRIBUTION };
  for (const auto& wl : supported_workloads) {
    deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 10>>(config, key_values, wl, shifting_insert_key_values);

#ifndef FAST_COMPILE
    if (config.pareto) {
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 1>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 2>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 3>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 4>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 5>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 6>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 7>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 8>>(config, key_values, wl, shifting_insert_key_values);
      deli_testbed::run_benchmark<BenchmarkRS<KeyType, PayloadType, 9>>(config, key_values, wl, shifting_insert_key_values);
    }
#endif // FAST_COMPILE
  }
}

}  // namespace deli_testbed
