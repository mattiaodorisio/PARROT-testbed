// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

/*
 * Simple benchmark that runs a mixture of point lookups and inserts on ALEX.
 */

#include "../indices/benchmark_alex.h"
#include "../indices/benchmark_lipp.h"
// #include "../indices/benchmark_dili.h"
#include "../indices/benchmark_deli.h"
#include "../indices/benchmark_pgm_static.h"
#include "../indices/benchmark_pgm_dynamic.h"

#include <iomanip>
#include <fstream>
#include <chrono>
#include <ctime>

#include "flags.h"
#include "utils.h"
#include "workload.h"

// TODO:
// - Implement missing workloads
// - Fix batches

static constexpr size_t NUM_BATCHES = 1;
std::mt19937_64 rand_gen(std::random_device{}());

template<Workload W, typename IndexType, typename KeyType, typename PayloadType>
double run_workload(IndexType& index, std::vector<std::pair<KeyType, PayloadType>>& key_values, size_t num_ops_per_batch) {

  // WORKLOAD: LOOKUP OF EXISTING KEYS
  if constexpr (W == LOOKUP_EXISTING) {
    std::vector<std::pair<KeyType, PayloadType>> lookup_pairs = get_existing_keys(key_values.begin(), key_values.end(), num_ops_per_batch);

    auto start_time = std::chrono::high_resolution_clock::now();
      for (const auto& [key, expected] : lookup_pairs) {
        PayloadType payload = index.lower_bound(key);
        if (expected != payload) {
            std::cerr << "Index: " << index.name() << " lookup (existing) error: key=" << key << ", expected=" << expected << ", got=" << payload << std::endl;
            return -1;
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  
  } else if constexpr (W == INSERT_IN_DISTRIBUTION) {
    auto keys = key_values | std::views::transform([](auto const& p) { return p.first; });
    std::vector<KeyType> insert_keys = get_non_existing_keys(keys.begin(), keys.end(), num_ops_per_batch);
    std::vector<std::pair<KeyType, PayloadType>> insert_pairs;
    for (const auto& key : insert_keys) {
      insert_pairs.emplace_back(key, rand_gen());
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto& [key, value] : insert_pairs) {
      index.insert(key, value);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  
  } else if constexpr (W == LOOKUP_IN_DISTRIBUTION) {
    auto keys = key_values | std::views::transform([](auto const& p) { return p.first; });
    std::vector<KeyType> non_existing_keys = get_non_existing_keys(keys.begin(), keys.end(), num_ops_per_batch);
    std::vector<std::pair<KeyType, PayloadType>> lookup_pairs(non_existing_keys.size());

    for (size_t i = 0; i < non_existing_keys.size(); i++) {
      auto expected_payload_it = std::lower_bound(
          key_values.begin(), key_values.end(), non_existing_keys[i],
          [](auto const& a, auto const& b) { return a.first < b; });
      PayloadType expected_payload = (expected_payload_it != key_values.end()) ? expected_payload_it->second : PayloadType{};
      lookup_pairs[i] = {non_existing_keys[i], expected_payload};
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
      for (const auto& [key, expected] : lookup_pairs) {
        PayloadType payload = index.lower_bound(key);
        if (expected != payload) {
            std::cerr << "Index: " << index.name() << " lookup (non-existing) error: key=" << key << ", expected=" << expected << ", got=" << payload << std::endl;
            throw std::runtime_error("Lookup error");
            return -1;
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  
  } else if constexpr (W == MIXED) {
    throw std::runtime_error("Workload not implemented");
  } else if constexpr (W == SHIFTING) {
    throw std::runtime_error("Workload not implemented");
  } else {
    throw std::runtime_error("Workload not implemented");
  }  
}

// Templated benchmark functions
template<typename IndexType, typename KeyType, typename PayloadType>
void bulk_load_benchmark(IndexType& index, std::vector<std::pair<KeyType, PayloadType>>& values) {
    std::sort(values.begin(), values.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    index.bulk_load(values.data(), values.size());
}

template<typename IndexType, typename KeyType, typename PayloadType>
double insert_benchmark(IndexType& index, const std::vector<std::pair<KeyType, PayloadType>>& key_value_pairs, int start_idx, int num_inserts) {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_inserts; i++) {
        index.insert(key_value_pairs[start_idx + i].first, key_value_pairs[start_idx + i].second);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
}

template<typename IndexType, typename KeyType, typename PayloadType>
void run_benchmark(const std::string& index_name, std::ofstream& out_file, std::vector<std::pair<KeyType, PayloadType>> key_values,
                   int batch_size, const std::string& lookup_distribution, Workload workload, double time_limit,
                   bool print_batch_stats, int max_batches = 10) {

    // Create the index and bulk load initial keys
    IndexType index;

    // Bulk load
    bulk_load_benchmark<IndexType, KeyType, PayloadType>(index, key_values);

    // Run workload
    size_t i = key_values.size();

    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    PayloadType sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);
    
    while (true) {
        batch_no++;

        double batch_time;
        switch (workload) {
          case LOOKUP_EXISTING: batch_time = run_workload<LOOKUP_EXISTING>(index, key_values, batch_size); break;
          case LOOKUP_IN_DISTRIBUTION: batch_time = run_workload<LOOKUP_IN_DISTRIBUTION>(index, key_values, batch_size); break;
          case INSERT_IN_DISTRIBUTION: batch_time = run_workload<INSERT_IN_DISTRIBUTION>(index, key_values, batch_size); break;
          default: throw std::runtime_error("Workload not implemented");
        }

        i += batch_size; // TODO: check this

        // Calculate batch statistics
        double batch_overall_throughput = (batch_time > 0) ? (batch_size / batch_time * 1e9) : 0.0;

        if (print_batch_stats) {
          std::cout << std::scientific << std::setprecision(3);
          std::cout << index_name << " " << workload_name << ": " << batch_size << " operations completed\n";
          std::cout << "Total time: " << batch_time / 1e9 << " seconds\n";
          std::cout << "Throughput: " << batch_overall_throughput << " ops/sec\n";
        }

        // Log results
        out_file << "RESULT "
                << "index_name=" << index_name << " "
                << "batch_no=" << batch_no << " "
                << "workload_type=" << workload_name(workload) << " "
                << "init_num_keys=" << key_values.size() << " "
                << "batch_operations=" << batch_size << " "
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

template <typename KeyType, typename PayloadType>
void execute(const std::string& keys_file_path,
             const std::string& keys_file_type,
             int batch_size,
             const std::string& lookup_distribution,
             double time_limit,
             bool print_batch_stats,
             std::ofstream& out_file) {

  // Read keys from file
  std::vector<KeyType> keys;
  if (keys_file_type == "binary") {
    keys = load_binary_data<KeyType>(keys_file_path);
  } else if (keys_file_type == "text") {
    // keys.resize(total_num_keys);
    // if (!load_text_data(keys.data(), total_num_keys, keys_file_path))
    //     throw std::runtime_error("Failed to load text data from " + keys_file_path);
  } else {
    std::cerr << "--keys_file_type must be either 'binary' or 'text'" << std::endl;
    exit(EXIT_FAILURE);
  }

  // Generate exponentially increasing init_num_keys sizes
  constexpr size_t min_size = 1 << 7;   // Starting size
  constexpr size_t max_size = 1 << 27;  // Maximum size
  
  std::cout << "\n=== Running benchmarks with exponentially increasing init_num_keys ===" << std::endl;

  // Define index types and names
  std::vector<std::string> index_names = {"ALEX", "LIPP", "DeLI", "PGM-Static", "PGM-Dynamic"};
  
  for (size_t current_init_key_size = min_size; current_init_key_size <= max_size; current_init_key_size *= 2) {
    std::cout << "\n=== Testing with " << current_init_key_size << " initial keys ===" << std::endl;

    // Create values array for current init size
    // Two vectors to be used depending on the index... TODO: improve this
    std::vector<std::pair<KeyType, PayloadType>> key_values(current_init_key_size);
    std::vector<std::pair<KeyType, PayloadType>> key_keys(current_init_key_size);
    for (size_t i = 0; i < key_values.size(); ++i) {
      key_values[i].first = keys[i];
      key_values[i].second = static_cast<PayloadType>(rand_gen());

      key_keys[i].first = key_keys[i].second = keys[i];
    }

    for (const auto& index_name : index_names) {
      std::cout << "\n--- Running workloads for " << index_name << " (init_keys=" << current_init_key_size << ") ---" << std::endl;
      
      if (index_name == "ALEX") {
        auto supported_workloads = BenchmarkALEX<KeyType, PayloadType>::supported_workloads();
        for (Workload workload : supported_workloads) {
          run_benchmark<BenchmarkALEX<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_values, batch_size,
              lookup_distribution, workload, time_limit, print_batch_stats, NUM_BATCHES);
        }
      }
      else if (index_name == "LIPP") {
        auto supported_workloads = BenchmarkLIPP<KeyType, PayloadType>::supported_workloads();
        for (Workload workload : supported_workloads) {
          run_benchmark<BenchmarkLIPP<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_values, batch_size,
              lookup_distribution, workload, time_limit, print_batch_stats, NUM_BATCHES);
        }
      }
      else if (index_name == "DeLI") {
        auto supported_workloads = BenchmarkDeLI<KeyType, PayloadType>::supported_workloads();
        for (Workload workload : supported_workloads) {
          run_benchmark<BenchmarkDeLI<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_keys, batch_size,
              lookup_distribution, workload, time_limit, print_batch_stats, NUM_BATCHES);
        }
      }
      else if (index_name == "PGM-Static") {
        auto supported_workloads = BenchmarkStaticPGM<KeyType, PayloadType>::supported_workloads();
        for (Workload workload : supported_workloads) {
          run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_keys, batch_size,
              lookup_distribution, workload, time_limit, print_batch_stats, NUM_BATCHES);
        }
      }
      else if (index_name == "PGM-Dynamic") {
        auto supported_workloads = BenchmarkDynamicPGM<KeyType, PayloadType>::supported_workloads();
        for (Workload workload : supported_workloads) {
          run_benchmark<BenchmarkDynamicPGM<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_keys, batch_size,
              lookup_distribution, workload, time_limit, print_batch_stats, NUM_BATCHES);
        }
      }
      else {
        throw std::runtime_error("Unsupported index: " + index_name);
      }
    }
  }
}

/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --keys_file_type         file type of keys_file (options: binary or text)
 * --batch_size             number of operations (lookup or insert) per batch
 *
 * Optional flags:
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 * --bench_output           custom filename for benchmark output (default: auto-generated with timestamp)
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  std::string keys_file_type = get_required(flags, "keys_file_type");
  auto batch_size = stoi(get_required(flags, "batch_size"));
  std::string lookup_distribution = get_with_default(flags, "lookup_distribution", "uniform");
  auto time_limit = stod(get_with_default(flags, "time_limit", "0.5"));
  bool print_batch_stats = get_boolean_flag(flags, "print_batch_stats");
  std::string bench_output = get_with_default(flags, "bench_output", "");

  // Check if input file does exist
  {
    std::ifstream infile(keys_file_path);
    if (!infile.good()) {
      throw std::runtime_error("The specified keys_file does not exist: " + keys_file_path);
    }
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
    throw std::runtime_error("Failed to create the benchmark output file: " + out_filename);
  }

  std::cout << "Benchmark results will be written to: " << out_filename << std::endl;

  // Call execute with appropriate key type based on filename suffix
  if (keys_file_path.ends_with("_uint32")) {
    execute<uint32_t, uint32_t>(keys_file_path, keys_file_type, batch_size,
                                          lookup_distribution, time_limit, print_batch_stats, out_file);
  } else if (keys_file_path.ends_with("_uint64")) {
    execute<uint64_t, uint64_t>(keys_file_path, keys_file_type, batch_size,
                                          lookup_distribution, time_limit, print_batch_stats, out_file);
  } else {
    throw std::runtime_error("Unsupported key type in filename. Expected suffixes: _uint32 or _uint64");
  }

  std::cout << "\nBenchmark results saved to: " << out_filename << std::endl;
  
  return 0;
}
