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
#include "../indices/benchmark_rs.h"
#include "../indices/benchmark_tlx.h"

#include <iomanip>
#include <fstream>
#include <chrono>
#include <ctime>
#include <vector>
#include <ranges>
#include <filesystem>
#include <unordered_set>

#include "flags.h"
#include "utils.h"
#include "benchmark.h"

template <typename KeyType, typename PayloadType>
void execute(const bench_config& config) {

  // Read keys from file
  std::vector<KeyType> keys;
  if (config.data_filename.ends_with(".txt")) {
    keys = utils::load_text_data<KeyType>(config.data_filename);
  } else {
    keys = utils::load_binary_data<KeyType>(config.data_filename);
  }

  if (keys.empty()) {
    std::cerr << "Error: No keys loaded from file: " << config.data_filename << std::endl;
    return;
  }

  std::cout << "\n=== Running benchmarks with exponentially increasing init_num_keys ===" << std::endl;

  // Define index types and names
  const std::vector<std::string> index_names = {"ALEX",
                                                "LIPP",
                                                "RS",
                                                "DeLI-Static",
                                                "DeLI-Dynamic",
                                                "DeLI-Static-Payload",
                                                "DeLI-Dynamic-Payload",
                                                "PGM-Static",
                                                "PGM-Dynamic",
                                                "TLX"
                                              };

  for (size_t current_init_key_size = (1 << config.min_size); current_init_key_size <= (1 << config.max_size); current_init_key_size *= 2) {
    std::cout << "\n=== Testing with " << current_init_key_size << " initial keys ===" << std::endl;

    // Check if we have enough keys loaded
    if (current_init_key_size > keys.size()) {
      std::cerr << "Error: Not enough keys loaded. Requested: " << current_init_key_size 
                << ", Available: " << keys.size() << std::endl;
      continue;  // Skip this size and try the next one
    }

    // Create values array for current init size
    // Two vectors: key-value and key_key to be used depending on the index... TODO: improve this
    std::vector<std::pair<KeyType, PayloadType>> key_values(current_init_key_size);
    std::vector<std::pair<KeyType, PayloadType>> key_keys(current_init_key_size);
    for (size_t i = 0; i < key_values.size(); ++i) {
      key_values[i].first = keys[i];
      key_values[i].second = static_cast<PayloadType>(rand_gen());

      key_keys[i].first = key_keys[i].second = keys[i];
    }

    // Sort by key
    std::sort(key_values.begin(), key_values.end(), [](auto const& a, auto const& b) { return a.first < b.first; });
    std::sort(key_keys.begin(), key_keys.end(), [](auto const& a, auto const& b) { return a.first < b.first; });

    // Build a full sorted shifting stream: initial prefix + append suffix
    std::vector<std::pair<KeyType, PayloadType>> shifting_key_values = key_values;
    std::vector<std::pair<KeyType, PayloadType>> shifting_key_keys = key_keys;
    const size_t inserts_per_batch = static_cast<size_t>(config.batch_size / 2 + config.batch_size % 2);
    const size_t required_append_keys = inserts_per_batch * static_cast<size_t>(config.max_batches + 1);
    shifting_key_values.reserve(current_init_key_size + required_append_keys);
    shifting_key_keys.reserve(current_init_key_size + required_append_keys);
    if (keys.size() >= current_init_key_size && required_append_keys > 0) {
      std::unordered_set<KeyType> seen_shifting_keys;
      seen_shifting_keys.reserve(current_init_key_size + required_append_keys);
      for (const auto& kv : key_values) {
        seen_shifting_keys.insert(kv.first);
      }

      for (size_t i = current_init_key_size; i < keys.size(); ++i) {
        if (shifting_key_values.size() >= current_init_key_size + required_append_keys) {
          break;
        }
        const KeyType key = keys[i];
        if (!seen_shifting_keys.insert(key).second) {
          continue;
        }
        shifting_key_values.emplace_back(key, static_cast<PayloadType>(rand_gen()));
        shifting_key_keys.emplace_back(key, key);
      }
    }

    std::sort(shifting_key_values.begin(), shifting_key_values.end(),
              [](auto const& a, auto const& b) { return a.first < b.first; });
    std::sort(shifting_key_keys.begin(), shifting_key_keys.end(),
              [](auto const& a, auto const& b) { return a.first < b.first; });
    
    for (const auto& index_name : index_names) {
      std::cout << "--- Running workloads for " << index_name << " (init_keys=" << current_init_key_size << ") ---" << std::endl;
      
      if (index_name == "ALEX") {
        deli_testbed::benchmark_alex<KeyType, PayloadType>(config, key_values, shifting_key_values);
      }
      else if (index_name == "LIPP") {
        deli_testbed::benchmark_lipp<KeyType, PayloadType>(config, key_values, shifting_key_values);
      }
      else if (index_name == "RS") {
        deli_testbed::benchmark_rs<KeyType, PayloadType>(config, key_keys, shifting_key_keys);
      }
      else if (index_name == "DeLI-Dynamic") {
        deli_testbed::benchmark_deli_dynamic<false, KeyType, PayloadType>(config, key_keys, shifting_key_keys);
      }
      else if (index_name == "DeLI-Static") {
        deli_testbed::benchmark_deli_static<false, KeyType, PayloadType>(config, key_keys, shifting_key_keys);
      }
      else if (index_name == "DeLI-Dynamic-Payload") {
        deli_testbed::benchmark_deli_dynamic<true, KeyType, PayloadType>(config, key_values, shifting_key_values);
      }
      else if (index_name == "DeLI-Static-Payload") {
        deli_testbed::benchmark_deli_static<true, KeyType, PayloadType>(config, key_values, shifting_key_values);
      }
      else if (index_name == "PGM-Static") {
        deli_testbed::benchmark_pgm_static<KeyType, PayloadType>(config, key_keys, shifting_key_keys);
      }
      else if (index_name == "PGM-Dynamic") {
        deli_testbed::benchmark_pgm_dynamic<KeyType, PayloadType>(config, key_values, shifting_key_values);
      }
      else if (index_name == "TLX") {
        deli_testbed::benchmark_tlx<KeyType, PayloadType>(config, key_keys, shifting_key_keys);
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
 * --batch_size             number of operations per batch
 * --output_folder          folder path for benchmark output
 * --max_batches            maximum number of measured batches before adaptive stop (default: 3)
 *
 * Optional flags:
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 * --clear_cache            whether to clear cache before each batch
 * --pareto                 whether to run the benchmark for all the parameters
 * --min_batches            minimum number of measured batches before adaptive stop (default: 2)
 * --rse_target             target relative standard error for early stop (default: 0.03)
 * --min_size               log of the minimum number of initial keys (default: 8 -> 2^8)
 * --max_size               log of the maximum number of initial keys (default: 20 -> 2^20)
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  auto batch_size = stoi(get_required(flags, "batch_size"));
  auto num_batches = get_with_default(flags, "num_batches", "");
  auto min_batches = stoi(get_with_default(flags, "min_batches", "2"));
  auto max_batches = stoi(get_with_default(flags, "max_batches", "3"));
  auto rse_target = stod(get_with_default(flags, "rse_target", "0.05"));
  std::string output_folder = get_required(flags, "output_folder");
  std::string lookup_distribution = get_with_default(flags, "lookup_distribution", "uniform");
  auto time_limit = stod(get_with_default(flags, "time_limit", "0.5"));
  bool print_batch_stats = get_boolean_flag(flags, "print_batch_stats");
  bool clear_cache = get_boolean_flag(flags, "clear_cache");
  bool pareto = get_boolean_flag(flags, "pareto");
  auto min_size = stoi(get_with_default(flags, "min_size", "8"));
  auto max_size = stoi(get_with_default(flags, "max_size", "20"));

  if (min_batches < 1) {
    throw std::runtime_error("--min_batches must be >= 1");
  }
  if (max_batches < min_batches) {
    throw std::runtime_error("--max_batches must be >= --min_batches");
  }
  if (rse_target <= 0.0) {
    throw std::runtime_error("--rse_target must be > 0");
  }
  if (!num_batches.empty()) {
    throw std::runtime_error("--num_batches is deprecated. Please use --min_batches and --max_batches instead.");
  }

  // Check if input file does exist
  {
    std::ifstream infile(keys_file_path);
    if (!infile.good()) {
      throw std::runtime_error("The specified keys_file does not exist: " + keys_file_path);
    }
  }

  // Extract dataset name from input file path
  std::filesystem::path input_path(keys_file_path);
  std::string dataset_name = input_path.stem().string();  // filename without extension
  
  // Create output folder if it doesn't exist
  std::filesystem::path output_dir(output_folder);
  if (!std::filesystem::exists(output_dir)) {
    std::filesystem::create_directories(output_dir);
  }
  
  // Create output filename with dataset name and timestamp
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << "benchmark_" << dataset_name << "_" << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S") << ".txt";
  
  std::filesystem::path out_filename = output_dir / ss.str();
  
  // Check if output file already exists
  if (std::filesystem::exists(out_filename)) {
    std::cerr << "Error: Output file already exists: " << out_filename << std::endl;
    std::cerr << "Benchmark terminated to avoid overwriting existing results." << std::endl;
    return 1;
  }
  
  std::ofstream out_file(out_filename);
  if (!out_file.is_open()) {
    throw std::runtime_error("Failed to create the benchmark output file: " + out_filename.string());
  }

  std::cout << "Benchmark results will be written to: " << out_filename << std::endl;

  // Prepare benchmark config object
  bench_config config {
      out_file: out_file,
      data_filename: keys_file_path,
      lookup_distribution: lookup_distribution,
      time_limit: time_limit,
      batch_size: batch_size,
      min_batches: min_batches,
      max_batches: max_batches,
      rse_target: rse_target,
      print_batch_stats: print_batch_stats,
      clear_cache: clear_cache,
      pareto: pareto,
      min_size: min_size,
      max_size: max_size
  };

  // Call execute with appropriate key type based on filename suffix
  if (keys_file_path.ends_with("_uint32")) {
    execute<uint32_t, uint32_t>(config);
  } else if (keys_file_path.ends_with("_uint64")) {
    execute<uint64_t, uint64_t>(config);
  } else if (keys_file_path.ends_with(".txt")) {
    execute<uint64_t, uint64_t>(config); // For text files, we assume uint64 keys
  } else {
    throw std::runtime_error("Unsupported key type in filename. Expected suffixes: _uint32, _uint64, or .txt");
  }

  std::cout << "\nBenchmark results saved to: " << out_filename << std::endl;
  
  return 0;
}
