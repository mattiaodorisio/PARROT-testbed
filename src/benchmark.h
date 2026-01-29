#pragma once

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <chrono>
#include <vector>
#include <ranges>
#include <iomanip>
#include <random>
#include <unordered_set>

#include "utils.h"
#include "workload.h"

namespace deli_testbed {

template <typename KeyType, typename PayloadType>
class Benchmark {
 public:
  Benchmark(std::vector<std::pair<KeyType, PayloadType>>& key_values,
            const size_t num_repeats = 1)
      : key_values_(key_values),
        num_repeats_(num_repeats),
        first_run_(true) {
  }

  // Workload-specific run methods
  template <class Index>
  double Run_lookup_existing(Index& index, size_t batch_size) {
    return RunWorkload<LOOKUP_EXISTING>(index, batch_size);
  }

  template <class Index>
  double Run_lookup_in_distribution(Index& index, size_t batch_size) {
    return RunWorkload<LOOKUP_IN_DISTRIBUTION>(index, batch_size);
  }

  template <class Index>
  double Run_insert_in_distribution(Index& index, size_t batch_size) {
    return RunWorkload<INSERT_IN_DISTRIBUTION>(index, batch_size);
  }

  template <class Index>
  void BulkLoad(Index& index) {
    // Sort the data before bulk loading
    std::sort(key_values_.begin(), key_values_.end(), 
              [](auto const& a, auto const& b) { return a.first < b.first; });
    index.bulk_load(key_values_.data(), key_values_.size());
  }

  template <class Index>
  void PrintResult(const Index& index, const std::string& workload_name, 
                   int batch_no, size_t init_num_keys, size_t batch_operations,
                   const std::string& lookup_distribution, double batch_time,
                   std::ofstream& out_file) {
    
    double batch_overall_throughput = (batch_time > 0) ? (batch_operations / batch_time * 1e9) : 0.0;

    // Log results in the same format as main.cpp
    out_file << "RESULT "
            << "index_name=" << Index::name() << " "
            << "batch_no=" << batch_no << " "
            << "workload_type=" << workload_name << " "
            << "init_num_keys=" << init_num_keys << " "
            << "batch_operations=" << batch_operations << " "
            << "lookup_distribution=" << lookup_distribution << " "
            << std::fixed << std::setprecision(2) << "throughput=" << batch_overall_throughput << " "
            << std::fixed << std::setprecision(6) << "total_time=" << batch_time / 1e9 << std::endl;
  }

 private:
  template <Workload W, class Index>
  double RunWorkload(Index& index, size_t batch_size) {
    if constexpr (W == LOOKUP_EXISTING) {
      return DoLookupExisting(index, batch_size);
    } else if constexpr (W == LOOKUP_IN_DISTRIBUTION) {
      return DoLookupInDistribution(index, batch_size);
    } else if constexpr (W == INSERT_IN_DISTRIBUTION) {
      return DoInsertInDistribution(index, batch_size);
    }
    return 0.0;
  }

  template <class Index>
  double DoLookupExisting(Index& index, size_t batch_size) {
    // Get existing keys from the dataset
    std::vector<std::pair<KeyType, PayloadType>> lookup_pairs = 
        get_existing_keys(key_values_.begin(), key_values_.end(), batch_size);

    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto& [key, expected] : lookup_pairs) {
      auto result = index.lower_bound(key);
      // Prevent optimization
      volatile auto dummy = result;
      (void)dummy;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  }

  template <class Index>
  double DoLookupInDistribution(Index& index, size_t batch_size) {
    auto keys = key_values_ | std::views::transform([](auto const& p) { return p.first; });
    std::vector<KeyType> lookup_keys = get_non_existing_keys(keys.begin(), keys.end(), batch_size);

    /* TODO: this is needed for checks
    std::vector<std::pair<KeyType, PayloadType>> lookup_pairs(lookup_keys.size());
    for (size_t i = 0; i < lookup_keys.size(); i++) {
      auto expected_payload_it = std::lower_bound(
          key_values_.begin(), key_values_.end(), lookup_keys[i],
          [](auto const& a, auto const& b) { return a.first < b; });
      PayloadType expected_payload = (expected_payload_it != key_values_.end()) ? expected_payload_it->second : PayloadType{};
      lookup_pairs[i] = {lookup_keys[i], expected_payload};
    }
    */

    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto& key : lookup_keys) {
      auto result = index.lower_bound(key);
      // Prevent optimization
      volatile auto dummy = result;
      (void)dummy;

      // TODO: introduce version with checks here...
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  }

  template <class Index>
  double DoInsertInDistribution(Index& index, size_t batch_size) {
    auto keys = key_values_ | std::views::transform([](auto const& p) { return p.first; });
    std::vector<KeyType> insert_keys = get_non_existing_keys(keys.begin(), keys.end(), batch_size);
    std::vector<std::pair<KeyType, PayloadType>> insert_pairs;
    for (const auto& key : insert_keys) {
      insert_pairs.emplace_back(key, rand_gen());
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto& [key, payload] : insert_pairs) {
      index.insert(key, payload);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  }

  std::vector<std::pair<KeyType, PayloadType>>& key_values_;
  size_t num_repeats_;
  bool first_run_;
};


template<class IndexWrapper, typename KeyType, typename PayloadType>
void run_benchmark(std::ofstream& out_file, std::vector<std::pair<KeyType, PayloadType>> key_values,
                   int batch_size, const std::string& lookup_distribution, Workload workload, double time_limit,
                   bool print_batch_stats, int max_batches = 10) {

    // Create the benchmark instance
    deli_testbed::Benchmark<KeyType, PayloadType> benchmark(key_values);
    
    // Create the index and bulk load initial keys
    IndexWrapper index;
    benchmark.BulkLoad(index);

    // Run workload
    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);
    
    while (true) {
        batch_no++;

        double batch_time;
        switch (workload) {
          case LOOKUP_EXISTING: 
            batch_time = benchmark.Run_lookup_existing(index, batch_size); 
            break;
          case LOOKUP_IN_DISTRIBUTION: 
            batch_time = benchmark.Run_lookup_in_distribution(index, batch_size); 
            break;
          case INSERT_IN_DISTRIBUTION: 
            batch_time = benchmark.Run_insert_in_distribution(index, batch_size); 
            break;
          default: 
            throw std::runtime_error("Workload not implemented");
        }

        if (print_batch_stats) {
          std::cout << std::scientific << std::setprecision(3);
          std::cout << index.name() << " " << workload_name(workload) << ": " << batch_size << " operations completed\n";
          std::cout << "Total time: " << batch_time / 1e9 << " seconds\n";
          double batch_overall_throughput = (batch_time > 0) ? (batch_size / batch_time * 1e9) : 0.0;
          std::cout << "Throughput: " << batch_overall_throughput << " ops/sec\n";
        }

        // Use the benchmark's print method for consistent formatting
        benchmark.PrintResult(index, workload_name(workload), batch_no, key_values.size(), 
                             batch_size, lookup_distribution, batch_time, out_file);

        // Check for workload end conditions
        if (batch_no >= max_batches) {
            break;
        }
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - workload_start_time)
                .count();
        if (workload_elapsed_time > time_limit * 1e9 * 60) {
            break;
        }
    }
}

}  // namespace deli_testbed
