// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

/*
 * Simple benchmark that runs a mixture of point lookups and inserts on ALEX.
 */

#include "../indices/benchmark_alex.h"
#include "../indices/benchmark_lipp.h"
// #include "../indices/benchmark_dili.h"
#include "../indices/benchmark_deli.h"
#include "../indices/benchmark_pgm.h"

#include <iomanip>
#include <fstream>
#include <chrono>
#include <ctime>

#include "flags.h"
#include "utils.h"

// Modify these if running your own workload
#define bench_KEY_TYPE double   
#define bench_PAYLOAD_TYPE double

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
void run_benchmark(const std::string& index_name, std::ofstream& out_file, KeyType* keys, std::pair<KeyType, PayloadType>* values,
                   int init_num_keys, int total_num_keys, int batch_size, double insert_frac,
                   const std::string& lookup_distribution, double time_limit, bool print_batch_stats,
                   std::mt19937_64& gen_payload, int max_batches = 10) {

    // Create the index and bulk load initial keys
    IndexType index;

    // Bulk load
    bulk_load_benchmark<IndexType, KeyType, PayloadType>(index, values, init_num_keys);

    // Run workload
    int i = init_num_keys;
    int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
    int num_lookups_per_batch = batch_size - num_inserts_per_batch;

    // Determine workload type for logging
    std::string workload_type;
    std::string workload_name;
    if (insert_frac == 0.0) {
        workload_type = "lookup_only";
        workload_name = "Lookup-only workload";
    } else if (insert_frac == 1.0) {
        workload_type = "insert_only";
        workload_name = "Insert-only workload";
    } else {
        workload_type = "mixed";
        workload_name = "Mixed workload";
    }

    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    PayloadType sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);
    
    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0 && num_lookups_per_batch > 0) {
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
            delete[] lookup_keys;
        }

        // Do inserts
        int num_actual_inserts = std::min(num_inserts_per_batch, total_num_keys - i);
        double batch_insert_time = 0.0;
        if (num_actual_inserts > 0) {
            batch_insert_time = insert_benchmark<IndexType, KeyType, PayloadType>(index, keys, i, num_actual_inserts, gen_payload);
            i += num_actual_inserts;
        }

        // Calculate batch statistics
        int num_batch_operations = num_lookups_per_batch + num_actual_inserts;
        double batch_time = batch_lookup_time + batch_insert_time;
        double batch_overall_throughput = (batch_time > 0) ? (num_batch_operations / batch_time * 1e9) : 0.0;

        if (print_batch_stats) {
          std::cout << std::scientific << std::setprecision(3);
          std::cout << index_name << " " << workload_name << ": " << num_batch_operations << " operations completed\n";
          std::cout << "Total time: " << batch_time / 1e9 << " seconds\n";
          std::cout << "Throughput: " << batch_overall_throughput << " ops/sec\n";
        }

        // Log results
        out_file << "RESULT "
                << "index_name=" << index_name << " "
                << "batch_no=" << batch_no << " "
                << "workload_type=" << workload_type << " "
                << "init_num_keys=" << init_num_keys << " "
                << "total_num_keys=" << total_num_keys << " "
                << "batch_operations=" << num_batch_operations << " "
                << "lookup_distribution=" << lookup_distribution << " "
                << std::fixed << std::setprecision(2) << "throughput=" << batch_overall_throughput << " "
                << std::fixed << std::setprecision(6) << "total_time=" << batch_time / 1e9 << std::endl;


        // Check for workload end conditions
        if (batch_no >= max_batches) {
            // End workload after specified number of batches
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

/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --keys_file_type         file type of keys_file (options: binary or text)
 * --init_num_keys          number of keys to bulk load with
 * --total_num_keys         total number of keys in the keys file
 * --batch_size             number of operations (lookup or insert) per batch
 *
 * Optional flags:
 * --insert_frac            fraction of operations that are inserts (for mixed workload)
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes (for mixed workload)
 * --print_batch_stats      whether to output stats for each batch (for mixed workload)
 * --bench_output           custom filename for benchmark output (default: auto-generated with timestamp)
 * --num_operations         number of operations for lookup-only and insert-only workloads (default: batch_size)
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
  std::string bench_output = get_with_default(flags, "bench_output", "");
  auto num_operations = stoi(get_with_default(flags, "num_operations", std::to_string(batch_size)));

  // Read keys from file
  auto keys = new bench_KEY_TYPE[total_num_keys];
  if (keys_file_type == "binary") {
    if (!load_binary_data(keys, total_num_keys, keys_file_path))
        throw std::runtime_error("Failed to load binary data from " + keys_file_path);
  } else if (keys_file_type == "text") {
    if (!load_text_data(keys, total_num_keys, keys_file_path))
        throw std::runtime_error("Failed to load text data from " + keys_file_path);
  } else {
    std::cerr << "--keys_file_type must be either 'binary' or 'text'"
              << std::endl;
    return 1;
  }

  // Combine bulk loaded keys with randomly generated payloads
  auto values = new std::pair<bench_KEY_TYPE, bench_PAYLOAD_TYPE>[init_num_keys];
  std::mt19937_64 gen_payload(std::random_device{}());
  for (int i = 0; i < init_num_keys; i++) {
    values[i].first = keys[i];
    values[i].second = static_cast<bench_PAYLOAD_TYPE>(gen_payload());
  }

  // Create benchmark output file with timestamp or use custom name
  std::string out_filename;
  if (bench_output.empty()) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << "benchmark_results_" << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S") << ".txt";
    out_filename = ss.str();
  } else {
    out_filename = bench_output;
  }
  
  std::ofstream out_file(out_filename);
  if (!out_file.is_open()) {
    std::cerr << "Failed to create the benchmark output file: " << out_filename << std::endl;
    return 1;
  }
  
  std::cout << "Benchmark results will be written to: " << out_filename << std::endl;
  
  // Calculate number of operations for single-operation workloads
  int max_inserts = total_num_keys - init_num_keys;
  int actual_num_operations = std::min(num_operations, max_inserts);
  
  std::cout << "\n=== Running benchmarks for all workload types ===" << std::endl;
  std::cout << "Initial keys: " << init_num_keys << std::endl;
  std::cout << "Operations per single workload: " << actual_num_operations << std::endl;
  std::cout << "Mixed workload batch size: " << batch_size << " (insert fraction: " << insert_frac << ")" << std::endl;

  // Define index types and names
  std::vector<std::string> index_names = {"ALEX", "LIPP", "DeLI", "PGM"};
  
  for (const auto& index_name : index_names) {
    std::cout << "\n--- Running workloads for " << index_name << " ---" << std::endl;
    
    if (index_name == "ALEX") {
      // Lookup-only workload
      std::cout << "\n1. Lookup-only workload" << std::endl;
      run_benchmark<BenchmarkALEX<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 0.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Insert-only workload
      std::cout << "\n2. Insert-only workload" << std::endl;
      run_benchmark<BenchmarkALEX<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 1.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Mixed workload
      std::cout << "\n3. Mixed workload (insert_frac=" << insert_frac << ")" << std::endl;
      run_benchmark<BenchmarkALEX<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, batch_size, insert_frac,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 10);
    }
    else if (index_name == "LIPP") {
      // Lookup-only workload
      std::cout << "\n1. Lookup-only workload" << std::endl;
      run_benchmark<BenchmarkLIPP<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 0.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Insert-only workload
      std::cout << "\n2. Insert-only workload" << std::endl;
      run_benchmark<BenchmarkLIPP<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 1.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Mixed workload
      std::cout << "\n3. Mixed workload (insert_frac=" << insert_frac << ")" << std::endl;
      run_benchmark<BenchmarkLIPP<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, batch_size, insert_frac,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 10);
    }
    else if (index_name == "DeLI") {
      // Lookup-only workload
      std::cout << "\n1. Lookup-only workload" << std::endl;
      run_benchmark<BenchmarkDeLI<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 0.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Insert-only workload
      std::cout << "\n2. Insert-only workload" << std::endl;
      run_benchmark<BenchmarkDeLI<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 1.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Mixed workload
      std::cout << "\n3. Mixed workload (insert_frac=" << insert_frac << ")" << std::endl;
      run_benchmark<BenchmarkDeLI<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, batch_size, insert_frac,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 10);
    }
    else if (index_name == "PGM") {
      // Lookup-only workload
      std::cout << "\n1. Lookup-only workload" << std::endl;
      run_benchmark<BenchmarkPGM<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 0.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Insert-only workload
      std::cout << "\n2. Insert-only workload" << std::endl;
      run_benchmark<BenchmarkPGM<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, actual_num_operations, 1.0,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 3);
      
      // Mixed workload
      std::cout << "\n3. Mixed workload (insert_frac=" << insert_frac << ")" << std::endl;
      run_benchmark<BenchmarkPGM<bench_KEY_TYPE, bench_PAYLOAD_TYPE>, bench_KEY_TYPE, bench_PAYLOAD_TYPE>(
          index_name, out_file, keys, values, init_num_keys, total_num_keys, batch_size, insert_frac,
          lookup_distribution, time_limit, print_batch_stats, gen_payload, 10);
    }
  }

  // Close out file
  out_file.close();
  std::cout << "\nBenchmark results saved to: " << out_filename << std::endl;

  delete[] keys;
  delete[] values;
}
