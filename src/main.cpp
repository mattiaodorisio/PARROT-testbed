// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

/*
 * Simple benchmark that runs a mixture of point lookups and inserts on ALEX.
 */

#include "../indices/benchmark_alex.h"
#include "../indices/benchmark_lipp.h"

#include <iomanip>

#include "flags.h"
#include "utils.h"

// Modify these if running your own workload
#define KEY_TYPE double
#define PAYLOAD_TYPE double

// Templated benchmark functions
template<typename IndexType, typename KeyType, typename PayloadType>
void bulk_load_benchmark(IndexType& index, std::pair<KeyType, PayloadType>* values, size_t num_keys) {
    std::sort(values, values + num_keys,
              [](auto const& a, auto const& b) { return a.first < b.first; });
    index.bulk_load(values, num_keys);
}

template<typename IndexType, typename KeyType, typename PayloadType>
double lookup_benchmark(IndexType& index, KeyType* lookup_keys, int num_lookups, PayloadType& sum) {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_lookups; i++) {
        PayloadType payload = index.lower_bound(lookup_keys[i]);
        if (payload) {
            sum += payload;
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
}

template<typename IndexType, typename KeyType, typename PayloadType>
double insert_benchmark(IndexType& index, KeyType* keys, int start_idx, int num_inserts, std::mt19937_64& gen_payload) {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_inserts; i++) {
        index.insert(keys[start_idx + i], static_cast<PayloadType>(gen_payload()));
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
}

template<typename IndexType, typename KeyType, typename PayloadType>
void run_benchmark(KeyType* keys, std::pair<KeyType, PayloadType>* values,
                   int init_num_keys, int total_num_keys, int batch_size, double insert_frac,
                   const std::string& lookup_distribution, double time_limit, bool print_batch_stats,
                   std::mt19937_64& gen_payload) {

    // Create the index and bulk load initial keys
    IndexType index;

    // Bulk load
    bulk_load_benchmark<IndexType, KeyType, PayloadType>(index, values, init_num_keys);

    // Run workload
    int i = init_num_keys;
    long long cumulative_inserts = 0;
    long long cumulative_lookups = 0;
    int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
    int num_lookups_per_batch = batch_size - num_inserts_per_batch;
    double cumulative_insert_time = 0;
    double cumulative_lookup_time = 0;

    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    PayloadType sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);
    
    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0) {
            KeyType* lookup_keys = nullptr;
            if (lookup_distribution == "uniform") {
                lookup_keys = get_search_keys(keys, i, num_lookups_per_batch);
            } else if (lookup_distribution == "zipf") {
                lookup_keys = get_search_keys_zipf(keys, i, num_lookups_per_batch);
            } else {
                std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'" << std::endl;
                return;
            }
            batch_lookup_time = lookup_benchmark<IndexType, KeyType, PayloadType>(index, lookup_keys, num_lookups_per_batch, sum);
            cumulative_lookup_time += batch_lookup_time;
            cumulative_lookups += num_lookups_per_batch;
            delete[] lookup_keys;
        }

        // Do inserts
        int num_actual_inserts = std::min(num_inserts_per_batch, total_num_keys - i);
        double batch_insert_time = insert_benchmark<IndexType, KeyType, PayloadType>(index, keys, i, num_actual_inserts, gen_payload);
        i += num_actual_inserts;
        cumulative_insert_time += batch_insert_time;
        cumulative_inserts += num_actual_inserts;

        if (print_batch_stats) {
            int num_batch_operations = num_lookups_per_batch + num_actual_inserts;
            double batch_time = batch_lookup_time + batch_insert_time;
            long long cumulative_operations = cumulative_lookups + cumulative_inserts;
            double cumulative_time = cumulative_lookup_time + cumulative_insert_time;
            std::cout << "Batch " << batch_no
                      << ", cumulative ops: " << cumulative_operations
                      << "\n\tbatch throughput:\t"
                      << num_lookups_per_batch / batch_lookup_time * 1e9
                      << " lookups/sec,\t"
                      << num_actual_inserts / batch_insert_time * 1e9
                      << " inserts/sec,\t" << num_batch_operations / batch_time * 1e9
                      << " ops/sec"
                      << "\n\tcumulative throughput:\t"
                      << cumulative_lookups / cumulative_lookup_time * 1e9
                      << " lookups/sec,\t"
                      << cumulative_inserts / cumulative_insert_time * 1e9
                      << " inserts/sec,\t"
                      << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                      << std::endl;
        }

        // Check for workload end conditions
        if (num_actual_inserts < num_inserts_per_batch) {
            // End if we have inserted all keys in a workload with inserts
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

    // Print final stats
    long long cumulative_operations = cumulative_lookups + cumulative_inserts;
    double cumulative_time = cumulative_lookup_time + cumulative_insert_time;
    std::cout << "Cumulative stats: " << batch_no << " batches, "
              << cumulative_operations << " ops (" << cumulative_lookups
              << " lookups, " << cumulative_inserts << " inserts)"
              << "\n\tcumulative throughput:\t"
              << cumulative_lookups / cumulative_lookup_time * 1e9
              << " lookups/sec,\t"
              << cumulative_inserts / cumulative_insert_time * 1e9
              << " inserts/sec,\t"
              << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
              << std::endl;
}

// Specialized benchmark functions for individual operations
template<typename IndexType, typename KeyType, typename PayloadType>
void run_lookup_only_benchmark(IndexType& index, KeyType* keys, int num_keys, 
                               const std::string& lookup_distribution, int num_lookups, bool print_stats = true) {
    PayloadType sum = 0;
    KeyType* lookup_keys = nullptr;
    
    if (lookup_distribution == "uniform") {
        lookup_keys = get_search_keys(keys, num_keys, num_lookups);
    } else if (lookup_distribution == "zipf") {
        lookup_keys = get_search_keys_zipf(keys, num_keys, num_lookups);
    } else {
        std::cerr << "Invalid lookup distribution: " << lookup_distribution << std::endl;
        return;
    }
    
    double lookup_time = lookup_benchmark<IndexType, KeyType, PayloadType>(index, lookup_keys, num_lookups, sum);
    
    if (print_stats) {
        std::cout << std::scientific << std::setprecision(3);
        std::cout << "Lookup benchmark: " << num_lookups << " lookups completed\n";
        std::cout << "Total time: " << lookup_time / 1e9 << " seconds\n";
        std::cout << "Throughput: " << num_lookups / lookup_time * 1e9 << " lookups/sec\n";
        std::cout << "Sum (to prevent optimization): " << sum << std::endl;
    }
    
    delete[] lookup_keys;
}

template<typename IndexType, typename KeyType, typename PayloadType>
void run_insert_only_benchmark(IndexType& index, KeyType* keys, int start_idx, int num_inserts, 
                               std::mt19937_64& gen_payload, bool print_stats = true) {
    double insert_time = insert_benchmark<IndexType, KeyType, PayloadType>(index, keys, start_idx, num_inserts, gen_payload);
    
    if (print_stats) {
        std::cout << std::scientific << std::setprecision(3);
        std::cout << "Insert benchmark: " << num_inserts << " inserts completed\n";
        std::cout << "Total time: " << insert_time / 1e9 << " seconds\n";
        std::cout << "Throughput: " << num_inserts / insert_time * 1e9 << " inserts/sec" << std::endl;
    }
}

/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --keys_file_type         file type of keys_file (options: binary or text)
 * --init_num_keys          number of keys to bulk load with
 * --total_num_keys         total number of keys in the keys file
 * --batch_size             number of operations (lookup or insert) per batch
 *
 * Optional flags:
 * --insert_frac            fraction of operations that are inserts (instead of
 * lookups)
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  std::string keys_file_type = get_required(flags, "keys_file_type");
  auto init_num_keys = stoi(get_required(flags, "init_num_keys"));
  auto total_num_keys = stoi(get_required(flags, "total_num_keys"));
  auto batch_size = stoi(get_required(flags, "batch_size"));
  auto insert_frac = stod(get_with_default(flags, "insert_frac", "0.5"));
  std::string lookup_distribution =
      get_with_default(flags, "lookup_distribution", "zipf");
  auto time_limit = stod(get_with_default(flags, "time_limit", "0.5"));
  bool print_batch_stats = get_boolean_flag(flags, "print_batch_stats");

  // Read keys from file
  auto keys = new KEY_TYPE[total_num_keys];
  if (keys_file_type == "binary") {
    load_binary_data(keys, total_num_keys, keys_file_path);
  } else if (keys_file_type == "text") {
    load_text_data(keys, total_num_keys, keys_file_path);
  } else {
    std::cerr << "--keys_file_type must be either 'binary' or 'text'"
              << std::endl;
    return 1;
  }

  // Combine bulk loaded keys with randomly generated payloads
  auto values = new std::pair<KEY_TYPE, PAYLOAD_TYPE>[init_num_keys];
  std::mt19937_64 gen_payload(std::random_device{}());
  for (int i = 0; i < init_num_keys; i++) {
    values[i].first = keys[i];
    values[i].second = static_cast<PAYLOAD_TYPE>(gen_payload());
  }

  // Run benchmark
  run_benchmark<BenchmarkALEX<KEY_TYPE, PAYLOAD_TYPE>, KEY_TYPE, PAYLOAD_TYPE>(
      keys, values, init_num_keys, total_num_keys, batch_size, insert_frac,
      lookup_distribution, time_limit, print_batch_stats, gen_payload);

  run_benchmark<BenchmarkLIPP<KEY_TYPE, PAYLOAD_TYPE>, KEY_TYPE, PAYLOAD_TYPE>(
      keys, values, init_num_keys, total_num_keys, batch_size, insert_frac,
      lookup_distribution, time_limit, print_batch_stats, gen_payload);

  delete[] keys;
  delete[] values;
}
