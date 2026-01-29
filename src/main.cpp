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
#include <vector>
#include <ranges>

#include "flags.h"
#include "utils.h"
#include "benchmark.h"

// TODO:
// - Implement missing workloads
// - Fix batches

static constexpr size_t NUM_BATCHES = 1;

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
  constexpr size_t max_size = 1 << 20;  // Maximum size
  
  std::cout << "\n=== Running benchmarks with exponentially increasing init_num_keys ===" << std::endl;

  // Define index types and names
  std::vector<std::string> index_names = {"ALEX", "LIPP", "DeLI", "PGM-Static", "PGM-Dynamic"};

  // Prepare benchmark config object
  bench_config config {
      out_file,
      batch_size,
      lookup_distribution,
      time_limit,
      print_batch_stats,
      NUM_BATCHES
  };
  
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
        deli_testbed::benchmark_alex<KeyType, PayloadType>(config, key_values);
      }
      else if (index_name == "LIPP") {
        deli_testbed::benchmark_lipp<KeyType, PayloadType>(config, key_values);
      }
      else if (index_name == "DeLI") {
        deli_testbed::benchmark_deli<KeyType, PayloadType>(config, key_values);
      }
      else if (index_name == "PGM-Static") {
        deli_testbed::benchmark_pgm_static<KeyType, PayloadType>(config, key_values);
      }
      else if (index_name == "PGM-Dynamic") {
        deli_testbed::benchmark_pgm_dynamic<KeyType, PayloadType>(config, key_values);
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
