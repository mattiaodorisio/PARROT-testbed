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

// TODO:
// - check workloads
// - distinguere existing e non-existing

static constexpr size_t NUM_BATCHES = 1;

// Templated benchmark functions
template<typename IndexType, typename KeyType, typename PayloadType>
void bulk_load_benchmark(IndexType& index, std::vector<std::pair<KeyType, PayloadType>>& values, size_t num_keys) {
    std::sort(values.begin(), values.begin() + num_keys,
              [](auto const& a, auto const& b) { return a.first < b.first; });
    index.bulk_load(values.data(), num_keys);
}

template<typename IndexType, typename KeyType, typename PayloadType>
double lookup_benchmark(IndexType& index, const std::vector<std::pair<KeyType, PayloadType>>& lookup_pairs, PayloadType& sum) {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto& [key, expected] : lookup_pairs) {
        PayloadType payload = index.lower_bound(key);
        if (expected != payload) {
            std::cerr << "Index: " << index.name() << " lookup error: key=" << key << ", expected=" << expected << ", got=" << payload << std::endl;
            return -1;
        }
        if (payload) {
            sum += payload;
        }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
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
void run_benchmark(const std::string& index_name, std::ofstream& out_file, std::vector<std::pair<KeyType, PayloadType>>& key_value_pairs,
                   int init_num_keys, int batch_size, double insert_frac,
                   const std::string& lookup_distribution, double time_limit, bool print_batch_stats,
                   std::mt19937_64& gen_payload, int max_batches = 10) {

    // Create the index and bulk load initial keys
    IndexType index;

    // Bulk load
    bulk_load_benchmark<IndexType, KeyType, PayloadType>(index, key_value_pairs, init_num_keys);

    // Run workload
    int i = init_num_keys;
    const int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
    const int num_lookups_per_batch = batch_size - num_inserts_per_batch;

    // check if there are enough keys for the specified workload
    if (num_inserts_per_batch * max_batches + init_num_keys > key_value_pairs.size()) {
        std::cerr << "Not enough keys for the specified workload: "
                  << "init_num_keys=" << init_num_keys << ", "
                  << "num_inserts_per_batch=" << num_inserts_per_batch << ", "
                  << "max_batches=" << max_batches << std::endl;
        return;
    }

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
            // Create a subset vector of keys from 0 to i
            std::vector<std::pair<KeyType, PayloadType>> lookup_pairs;
            if (lookup_distribution == "uniform") {
                lookup_pairs = get_search_keys(key_value_pairs.begin(), key_value_pairs.begin() + i, num_lookups_per_batch);
            } else if (lookup_distribution == "zipf") {
                lookup_pairs = get_search_keys_zipf(key_value_pairs.begin(), key_value_pairs.begin() + i, num_lookups_per_batch);
            } else {
                std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'" << std::endl;
                exit(EXIT_FAILURE);
            }
            batch_lookup_time = lookup_benchmark<IndexType, KeyType, PayloadType>(index, lookup_pairs, sum);
            if (batch_lookup_time < 0) {
                std::cerr << "Lookup benchmark failed" << std::endl;
                return;
            }
        }

        // Do inserts
        double batch_insert_time = 0.0;
        if (num_inserts_per_batch > 0 && index.is_dynamic()) {
            batch_insert_time = insert_benchmark<IndexType, KeyType, PayloadType>(index, key_value_pairs, i, num_inserts_per_batch);
            i += num_inserts_per_batch;
        }

        // Calculate batch statistics
        int num_batch_operations = num_lookups_per_batch + num_inserts_per_batch;
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

template <typename KeyType, typename PayloadType>
void execute(const std::string& keys_file_path,
             const std::string& keys_file_type,
             int batch_size,
             double insert_frac,
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

  std::mt19937_64 gen_payload(std::random_device{}());
  
  // Generate exponentially increasing init_num_keys values
  constexpr size_t base_size = 1 << 7;  // Starting size
  constexpr size_t max_size = 1 << 20;  // Maximum size
  
  std::cout << "\n=== Running benchmarks with exponentially increasing init_num_keys ===" << std::endl;
  std::cout << "Mixed workload batch size: " << batch_size << " (insert fraction: " << insert_frac << ")" << std::endl;

  // Define index types and names
  std::vector<std::string> index_names = {"ALEX", "LIPP", "DeLI", "PGM-Static"};

  // Create values array for current init size
  // Two vectors to be used depending on the index... TODO: improve this
  std::vector<std::pair<KeyType, PayloadType>> key_values(keys.size());
  std::vector<std::pair<KeyType, PayloadType>> key_keys(keys.size());
  for (int i = 0; i < key_values.size(); i++) {
    key_values[i].first = keys[i];
    key_values[i].second = static_cast<PayloadType>(gen_payload());

    key_keys[i].first = key_keys[i].second = keys[i];
  }
  
  for (size_t current_init_key_size = base_size; current_init_key_size <= max_size; current_init_key_size *= 2) {
    std::cout << "\n=== Testing with " << current_init_key_size << " initial keys ===" << std::endl;

    std::array<double, 3> insert_fractions = {0.0, 1.0, insert_frac};

    for (const auto& index_name : index_names) {
      std::cout << "\n--- Running workloads for " << index_name << " (init_keys=" << current_init_key_size << ") ---" << std::endl;
      
      if (index_name == "ALEX") {
        for (const auto& insert_frac : insert_fractions) {
          std::cout << "\nRunning workload with insert_frac=" << insert_frac << std::endl;
          run_benchmark<BenchmarkALEX<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_values, current_init_key_size, batch_size, insert_frac,
              lookup_distribution, time_limit, print_batch_stats, gen_payload, NUM_BATCHES);
        }
      }
      else if (index_name == "LIPP") {
        for (const auto& insert_frac : insert_fractions) {
          std::cout << "\nRunning workload with insert_frac=" << insert_frac << std::endl;
          run_benchmark<BenchmarkLIPP<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_values, current_init_key_size, batch_size, insert_frac,
              lookup_distribution, time_limit, print_batch_stats, gen_payload, NUM_BATCHES);
        }
      }
      else if (index_name == "DeLI") {
        for (const auto& insert_frac : insert_fractions) {
          std::cout << "\nRunning workload with insert_frac=" << insert_frac << std::endl;
          run_benchmark<BenchmarkDeLI<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_keys, current_init_key_size, batch_size, insert_frac,
              lookup_distribution, time_limit, print_batch_stats, gen_payload, NUM_BATCHES);
        }
      }
      else if (index_name == "PGM-Static") {
        for (const auto& insert_frac : insert_fractions) {
          std::cout << "\nRunning workload with insert_frac=" << insert_frac << std::endl;
          run_benchmark<BenchmarkStaticPGM<KeyType, PayloadType>, KeyType, PayloadType>(
              index_name, out_file, key_keys, current_init_key_size, batch_size, insert_frac,
              lookup_distribution, time_limit, print_batch_stats, gen_payload, NUM_BATCHES);
        }
      }
    }
  }
}

/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --keys_file_type         file type of keys_file (options: binary or text)
 * --init_num_keys          number of keys to bulk load with
 * --batch_size             number of operations (lookup or insert) per batch
 *
 * Optional flags:
 * --insert_frac            fraction of operations that are inserts (for mixed workload)
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes (for mixed workload)
 * --print_batch_stats      whether to output stats for each batch (for mixed workload)
 * --bench_output           custom filename for benchmark output (default: auto-generated with timestamp)
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  std::string keys_file_type = get_required(flags, "keys_file_type");
  auto batch_size = stoi(get_required(flags, "batch_size"));
  auto insert_frac = stod(get_with_default(flags, "insert_frac", "0.5"));
  std::string lookup_distribution = get_with_default(flags, "lookup_distribution", "zipf");
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
    execute<uint32_t, uint32_t>(keys_file_path, keys_file_type, batch_size, insert_frac,
                                          lookup_distribution, time_limit, print_batch_stats, out_file);
  } else if (keys_file_path.ends_with("_uint64")) {
    execute<uint64_t, uint64_t>(keys_file_path, keys_file_type, batch_size, insert_frac,
                                          lookup_distribution, time_limit, print_batch_stats, out_file);
  } else {
    throw std::runtime_error("Unsupported key type in filename. Expected suffixes: _uint32 or _uint64");
  }

  std::cout << "\nBenchmark results saved to: " << out_filename << std::endl;
  
  return 0;
}
