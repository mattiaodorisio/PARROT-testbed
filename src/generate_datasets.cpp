// Taken from https://github.com/LorenzoBellomo/SortedStaticIndexBenchmark/blob/main/scripts/elaborate_datasets.cpp

#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <cassert>
#include <random>
#include <stdint.h>
#include <cmath>
#include <unordered_set>
#include "zipf.h"
#include "flags.h"

#define M50 50000000
#define M1 1000000

std::vector<uint32_t> read_bin32_file(const std::string& filename) {
    std::ifstream ifs(filename, std::ios::binary);

    uint64_t N = 0;
    ifs.read(reinterpret_cast<char*>(&N), sizeof(uint64_t));

    std::vector<uint32_t> buf(N);
    ifs.read(reinterpret_cast<char*>(buf.data()), N * sizeof(uint32_t));

    // CHANGING LAST VALUES FROM UINT32_MAX to UINT32_MAX-1 to avoid issues for libraries that use it as "inf".
    for (auto i = buf.size() - 1; i >= 0; i--) {
        if (buf[i] == UINT32_MAX)
            buf[i] = UINT32_MAX - 1;
        else
            break;
    }
    return buf;
}

std::vector<uint64_t> read_bin64_file(const std::string& filename) {
    std::ifstream ifs(filename, std::ios::binary);

    uint64_t N = 0;
    ifs.read(reinterpret_cast<char*>(&N), sizeof(uint64_t));

    std::vector<uint64_t> buf(N);
    ifs.read(reinterpret_cast<char*>(buf.data()), N * sizeof(uint64_t));
    // CHANGING LAST VALUES FROM UINT64_MAX to UINT64_MAX-1 to avoid issues for libraries that use it as "inf".
    for (auto i = buf.size() - 1; i >= 0; i--) {
        if (buf[i] == UINT64_MAX)
            buf[i] = UINT64_MAX - 1;
        else
            break;
    }
    return buf;
}

void write_bin32_file(std::string fname, std::vector<uint32_t> data) {
    std::ofstream out(fname, std::ios_base::binary);
    uint64_t size = data.size();
    out.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    out.write(reinterpret_cast<char*>(data.data()), size * sizeof(uint32_t));
}

void write_bin64_file(std::string fname, std::vector<uint64_t> data) {
    std::ofstream out(fname, std::ios_base::binary);
    uint64_t size = data.size();
    out.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    out.write(reinterpret_cast<char*>(data.data()), size * sizeof(uint64_t));
}

enum class UniqueMode {
    Adjust,
    Rejection
};

void make_unique_adjusted_samples(std::vector<uint32_t>& data, uint32_t max_allowed, uint32_t shuffle_seed) {
    if (data.empty()) {
        return;
    }

    std::sort(data.begin(), data.end());

    for (size_t i = 1; i < data.size(); ++i) {
        if (data[i] <= data[i - 1]) {
            data[i] = data[i - 1] + 1;
        }
    }

    if (data.back() > max_allowed) {
        data.back() = max_allowed;
        for (size_t i = data.size() - 1; i > 0; --i) {
            if (data[i - 1] >= data[i]) {
                data[i - 1] = data[i] - 1;
            }
        }
    }

    std::default_random_engine shuffler(shuffle_seed);
    std::shuffle(data.begin(), data.end(), shuffler);
}

std::vector<uint32_t> generate_uniform_distr(size_t size, UniqueMode unique_mode) {
    std::default_random_engine generator(0);
    std::uniform_int_distribution<uint32_t> distribution(0, UINT32_MAX);

    std::vector<uint32_t> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<uint32_t> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            uint32_t value = distribution(generator);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<uint32_t> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in lognormal distribution with rejection sampling"); 
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = distribution(generator);
    }
    make_unique_adjusted_samples(data, UINT32_MAX, 100);
    return data;
}

std::vector<uint32_t> generate_normal_distr(size_t size, UniqueMode unique_mode) {
    const uint32_t max_allowed = UINT32_MAX >> 1;
    std::default_random_engine generator(1);
    std::normal_distribution<double> distribution(UINT32_MAX / 2, UINT32_MAX / 4);

    std::vector<uint32_t> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<uint32_t> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator);
            } while (sample < 0 || sample > max_allowed);

            uint32_t value = static_cast<uint32_t>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<uint32_t> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in lognormal distribution with rejection sampling"); 
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator);
        } while (sample < 0 || sample > max_allowed);
        data[i] = static_cast<uint32_t>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 101);
    return data;
}

std::vector<uint32_t> generate_lognormal_distr(size_t size, UniqueMode unique_mode) {
    const uint32_t max_allowed = UINT32_MAX >> 1;
    std::default_random_engine generator(2);
    std::lognormal_distribution<double> distribution(0, 0.5);

    std::vector<uint32_t> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<uint32_t> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator) * UINT32_MAX / 5;
            } while (sample < 0 || sample > max_allowed);

            uint32_t value = static_cast<uint32_t>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<uint32_t> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in lognormal distribution with rejection sampling"); 
        }

        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator) * UINT32_MAX / 5;
        } while (sample < 0 || sample > max_allowed);
        data[i] = static_cast<uint32_t>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 102);
    return data;
}

std::vector<uint32_t> generate_exponential_distr(size_t size, UniqueMode unique_mode) {
    const uint32_t max_allowed = UINT32_MAX >> 1;
    std::default_random_engine generator(3);
    std::exponential_distribution<double> distribution(2);

    std::vector<uint32_t> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<uint32_t> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator) * UINT32_MAX / 5;
            } while (sample < 0 || sample > max_allowed);

            uint32_t value = static_cast<uint32_t>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<uint32_t> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in lognormal distribution with rejection sampling"); 
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator) * UINT32_MAX / 5;
        } while (sample < 0 || sample > max_allowed);
        data[i] = static_cast<uint32_t>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 103);
    return data;
}

std::vector<uint32_t> generate_chisquared_distr(size_t size, UniqueMode unique_mode) {
    const uint32_t max_allowed = UINT32_MAX >> 1;
    std::default_random_engine generator(4);
    std::chi_squared_distribution<double> distribution(1);

    std::vector<uint32_t> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<uint32_t> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator) * UINT32_MAX / 10;
            } while (sample < 0 || sample > max_allowed);

            uint32_t value = static_cast<uint32_t>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<uint32_t> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in chi-squared distribution with rejection sampling"); 
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator) * UINT32_MAX / 10;
        } while (sample < 0 || sample > max_allowed);
        data[i] = static_cast<uint32_t>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 104);
    return data;
}


std::vector<uint32_t> generate_mix_of_gauss_distr(size_t size, UniqueMode unique_mode, size_t num_gauss = 5) {
    const uint32_t max_allowed = UINT32_MAX >> 1;
    std::default_random_engine generator(5);
    
    // Generate means concentrated around the center of the domain.
    const double domain_center = max_allowed / 2.0;
    const double mean_spread = max_allowed / 10.0;
    std::normal_distribution<double> mean_dist(domain_center, mean_spread);
    std::vector<double> means(num_gauss);
    for (size_t i = 0; i < num_gauss; ++i) {
        double mean_sample;
        do {
            mean_sample = mean_dist(generator);
        } while (mean_sample < 0.0 || mean_sample > max_allowed);
        means[i] = mean_sample;
    }
    
    // Use domain-scaled standard deviations to avoid over-concentrated outputs.
    std::uniform_real_distribution<double> stddev_dist(max_allowed / 200.0, max_allowed / 20.0);
    std::vector<double> stdevs(num_gauss);
    for (size_t i = 0; i < num_gauss; ++i) {
        stdevs[i] = stddev_dist(generator);
    }
    
    // Generate weights (uniform between 0 and 1)
    std::uniform_real_distribution<double> weight_dist(0.0, 1.0);
    std::vector<double> weights(num_gauss);
    for (size_t i = 0; i < num_gauss; ++i) {
        weights[i] = weight_dist(generator);
    }
    
    // Normalize the weights
    double sum_of_weights = std::accumulate(weights.begin(), weights.end(), 0.0);
    std::for_each(weights.begin(), weights.end(),
                  [sum_of_weights](double& w) { w /= sum_of_weights; });
    
    std::vector<uint32_t> data;
    data.reserve(size);
    
    // Initialize random distribution selector
    std::discrete_distribution<int> index_selector(weights.begin(), weights.end());
    
    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<uint32_t> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            auto random_idx = index_selector(generator);
            std::normal_distribution<double> distribution(means[random_idx], stdevs[random_idx]);
            
            double sample;
            do {
                sample = distribution(generator);
            } while (sample < 0 || sample > max_allowed);
            
            uint32_t value = static_cast<uint32_t>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<uint32_t> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in mixture of gaussians distribution with rejection sampling");
        }
        return data;
    }
    
    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        auto random_idx = index_selector(generator);
        std::normal_distribution<double> distribution(means[random_idx], stdevs[random_idx]);
        
        double sample;
        do {
            sample = distribution(generator);
        } while (sample < 0 || sample > max_allowed);
        data[i] = static_cast<uint32_t>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 105);
    return data;
}

std::vector<uint32_t> generate_zipf_distr(size_t size) {
    std::vector<uint32_t> data(size);
    ScrambledZipfianGenerator zipf_gen(size);
    for (size_t i = 0; i < size; ++i) {
      data[i] = zipf_gen.nextValue();
    }
    return data;
}

template<typename T>
void print_stats(std::vector<T> data) {
    std::cout << "SIZE: " << data.size() << std::endl;
    if (data.empty()) {
        std::cout << "AVERAGE GAP: 0" << std::endl;
        std::cout << "# duplicates: 0" << std::endl;
        std::cout << "===========================================" << std::endl;
        return;
    }

    std::sort(data.begin(), data.end());

    auto num_dup = 0;
    uint64_t tot_gap = 0;
    auto curr = data.begin() + 1;
    T prev_elem = data[0];
    while(curr != data.end()) {
        if (prev_elem == *curr)
            num_dup++;

        assert (*curr >= prev_elem);
        tot_gap += (*curr - prev_elem);
        prev_elem = *curr;
        curr++;
    }
    std::cout << "AVERAGE GAP: " << (tot_gap / (double)(data.size() - 1)) << std::endl;
    std::cout << "# duplicates: " << num_dup << std::endl;
    std::cout << "===========================================" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " --data_dir=<path> [--print_stats] [--unique_mode=<adjust|rejection>]" << std::endl;
        return 1;
    }
    
    auto flags = parse_flags(argc, argv);
    get_required(flags, "data_dir");
    bool print_stats_flag = get_boolean_flag(flags, "print_stats");
    std::string mode = get_with_default(flags, "unique_mode", "adjust");
    if (mode != "adjust" && mode != "rejection") {
        std::cout << "Unknown --unique_mode [rejection|adjust]" << std::endl;
    }
    UniqueMode unique_mode = mode == "rejection" ? UniqueMode::Rejection
                                                 : UniqueMode::Adjust;
    
    std::vector<uint32_t> data = generate_normal_distr(M50, unique_mode);
    write_bin32_file("../data/normal_50M_uint32", data);
    std::cout << "normal_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_exponential_distr(M50, unique_mode);
    write_bin32_file("../data/exponential_50M_uint32", data);
    std::cout << "exponential_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_lognormal_distr(M50, unique_mode);
    write_bin32_file("../data/lognormal_50M_uint32", data);
    std::cout << "lognormal_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_mix_of_gauss_distr(M50, unique_mode);
    write_bin32_file("../data/mix_gauss_50M_uint32", data);
    std::cout << "mix_gauss_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_zipf_distr(M50);
    write_bin32_file("../data/zipf_50M_uint32", data);
    std::cout << "zipf_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = generate_uniform_distr(M50, unique_mode);
    write_bin32_file("../data/uniform_50M_uint32", data);
    std::cout << "uniform_50M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    data = read_bin32_file("../data/books_200M_uint32");
    // REWRITING THE FILE ON DISK TO REPLACE UINT32_MAX with UINT32_MAX - 1 for safety for some libs
    for (size_t i = data.size() - 1; i >= 0; --i) {
        if (data[i] == UINT32_MAX)
            data[i] = UINT32_MAX - 1;
        else
            break;
    }
    write_bin32_file("../data/books_200M_uint32", data); 
    std::cout << "books_200M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    return 0;
}
