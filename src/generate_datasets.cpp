// Taken from https://github.com/LorenzoBellomo/SortedStaticIndexBenchmark/blob/main/scripts/elaborate_datasets.cpp

#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <cassert>
#include <random>
#include <stdint.h>
#include <cmath>
#include <limits>
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

template<typename T>
void make_unique_adjusted_samples(std::vector<T>& data, T max_allowed, uint32_t shuffle_seed) {
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

template<typename T>
std::vector<T> generate_uniform_distr(size_t size, UniqueMode unique_mode) {
    std::default_random_engine generator(0);
    std::uniform_int_distribution<T> distribution(0, std::numeric_limits<T>::max());

    std::vector<T> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<T> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            T value = distribution(generator);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<T> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in uniform distribution with rejection sampling");
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = distribution(generator);
    }
    make_unique_adjusted_samples(data, std::numeric_limits<T>::max(), 100);
    return data;
}

template<typename T>
std::vector<T> generate_normal_distr(size_t size, UniqueMode unique_mode) {
    const T max_allowed = std::numeric_limits<T>::max() >> 1;
    const double dmax = static_cast<double>(std::numeric_limits<T>::max());
    std::default_random_engine generator(1);
    std::normal_distribution<double> distribution(dmax / 2, dmax / 4);

    std::vector<T> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<T> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator);
            } while (sample < 0 || sample > static_cast<double>(max_allowed));

            T value = static_cast<T>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<T> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in normal distribution with rejection sampling");
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator);
        } while (sample < 0 || sample > static_cast<double>(max_allowed));
        data[i] = static_cast<T>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 101);
    return data;
}

template<typename T>
std::vector<T> generate_lognormal_distr(size_t size, UniqueMode unique_mode) {
    const T max_allowed = std::numeric_limits<T>::max() >> 1;
    const double dmax = static_cast<double>(std::numeric_limits<T>::max());
    std::default_random_engine generator(2);
    std::lognormal_distribution<double> distribution(0, 0.5);

    std::vector<T> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<T> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator) * dmax / 5;
            } while (sample < 0 || sample > static_cast<double>(max_allowed));

            T value = static_cast<T>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<T> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in lognormal distribution with rejection sampling");
        }

        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator) * dmax / 5;
        } while (sample < 0 || sample > static_cast<double>(max_allowed));
        data[i] = static_cast<T>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 102);
    return data;
}

template<typename T>
std::vector<T> generate_exponential_distr(size_t size, UniqueMode unique_mode) {
    const T max_allowed = std::numeric_limits<T>::max() >> 1;
    const double dmax = static_cast<double>(std::numeric_limits<T>::max());
    std::default_random_engine generator(3);
    std::exponential_distribution<double> distribution(2);

    std::vector<T> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<T> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator) * dmax / 5;
            } while (sample < 0 || sample > static_cast<double>(max_allowed));

            T value = static_cast<T>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<T> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in exponential distribution with rejection sampling");
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator) * dmax / 5;
        } while (sample < 0 || sample > static_cast<double>(max_allowed));
        data[i] = static_cast<T>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 103);
    return data;
}

template<typename T>
std::vector<T> generate_chisquared_distr(size_t size, UniqueMode unique_mode) {
    const T max_allowed = std::numeric_limits<T>::max() >> 1;
    const double dmax = static_cast<double>(std::numeric_limits<T>::max());
    std::default_random_engine generator(4);
    std::chi_squared_distribution<double> distribution(1);

    std::vector<T> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<T> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = distribution(generator) * dmax / 10;
            } while (sample < 0 || sample > static_cast<double>(max_allowed));

            T value = static_cast<T>(sample);
            if (seen.insert(value).second) {
                data.push_back(value);
            }
        }
        // Check unique constraint
        std::unordered_set<T> check_unique(data.begin(), data.end());
        if (check_unique.size() != data.size()) {
            throw std::logic_error("Duplicate values found in chi-squared distribution with rejection sampling");
        }
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = distribution(generator) * dmax / 10;
        } while (sample < 0 || sample > static_cast<double>(max_allowed));
        data[i] = static_cast<T>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 104);
    return data;
}


template<typename T>
std::vector<T> generate_mix_of_gauss_distr(size_t size, UniqueMode unique_mode) {
    const double dmax       = static_cast<double>(std::numeric_limits<T>::max() >> 1);
    const T      max_allowed = static_cast<T>(dmax);
    std::default_random_engine generator(5);

    // Two narrow, well-separated Gaussians:
    //   Component 0 (70%): mean = dmax/4,  sigma = dmax/50
    //   Component 1 (30%): mean = 3*dmax/4, sigma = dmax/100
    // The gap between the 3-sigma extents of the two peaks is ~40% of the domain.
    std::normal_distribution<double> dist0(dmax * 0.25, dmax / 50.0);
    std::normal_distribution<double> dist1(dmax * 0.75, dmax / 100.0);
    std::bernoulli_distribution coin(0.70);  // true → component 0 (70%)

    auto sample_one = [&]() -> double {
        return coin(generator) ? dist0(generator) : dist1(generator);
    };

    std::vector<T> data;
    data.reserve(size);

    if (unique_mode == UniqueMode::Rejection) {
        std::unordered_set<T> seen;
        seen.reserve(size * 2);
        while (data.size() < size) {
            double sample;
            do {
                sample = sample_one();
            } while (sample < 0 || sample > static_cast<double>(max_allowed));
            T value = static_cast<T>(sample);
            if (seen.insert(value).second)
                data.push_back(value);
        }
        std::unordered_set<T> check(data.begin(), data.end());
        if (check.size() != data.size())
            throw std::logic_error("Duplicate values found in mix_gauss distribution with rejection sampling");
        return data;
    }

    data.resize(size);
    for (size_t i = 0; i < size; ++i) {
        double sample;
        do {
            sample = sample_one();
        } while (sample < 0 || sample > static_cast<double>(max_allowed));
        data[i] = static_cast<T>(sample);
    }
    make_unique_adjusted_samples(data, max_allowed, 105);
    return data;
}

template<typename T>
std::vector<T> generate_zipf_distr(size_t size) {
    std::vector<T> data(size);
    zipf_distr(data.begin(), data.end(), 0.99, 1e8);

    // Only adjustment method for the zipf
    make_unique_adjusted_samples(data, std::numeric_limits<T>::max(), 106);

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

template<typename T>
void generate_all_synthetic(const std::string& data_dir, UniqueMode unique_mode, bool print_stats_flag) {
    constexpr const char* suffix = std::is_same_v<T, uint32_t> ? "uint32" : "uint64";
    auto write_file = [&](const std::string& path, std::vector<T>& data) {
        if constexpr (std::is_same_v<T, uint32_t>)
            write_bin32_file(path, data);
        else
            write_bin64_file(path, data);
    };

    auto run = [&](const std::string& name, std::vector<T> data) {
        std::string path = data_dir + "/" + name + "_50M_" + suffix;
        write_file(path, data);
        std::cout << name << "_50M_" << suffix << std::endl;
        if (print_stats_flag) print_stats(data);
    };

    run("normal",      generate_normal_distr<T>(M50, unique_mode));
    run("exponential", generate_exponential_distr<T>(M50, unique_mode));
    run("lognormal",   generate_lognormal_distr<T>(M50, unique_mode));
    run("mix_gauss",   generate_mix_of_gauss_distr<T>(M50, unique_mode));
    run("zipf",        generate_zipf_distr<T>(M50));
    run("uniform",     generate_uniform_distr<T>(M50, unique_mode));
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " --data_dir=<path> [--print_stats] [--unique_mode=<adjust|rejection>]" << std::endl;
        return 1;
    }

    auto flags = parse_flags(argc, argv);
    std::string data_dir = get_required(flags, "data_dir");
    bool print_stats_flag = get_boolean_flag(flags, "print_stats");
    std::string mode = get_with_default(flags, "unique_mode", "adjust");
    if (mode != "adjust" && mode != "rejection") {
        std::cout << "Unknown --unique_mode [rejection|adjust]" << std::endl;
    }
    UniqueMode unique_mode = mode == "rejection" ? UniqueMode::Rejection
                                                 : UniqueMode::Adjust;

    generate_all_synthetic<uint32_t>(data_dir, unique_mode, print_stats_flag);
    generate_all_synthetic<uint64_t>(data_dir, unique_mode, print_stats_flag);

    std::vector<uint32_t> data = read_bin32_file(data_dir + "/books_200M_uint32");
    // REWRITING THE FILE ON DISK TO REPLACE UINT32_MAX with UINT32_MAX - 1 for safety for some libs
    for (size_t i = data.size() - 1; i >= 0; --i) {
        if (data[i] == UINT32_MAX)
            data[i] = UINT32_MAX - 1;
        else
            break;
    }
    write_bin32_file(data_dir + "/books_200M_uint32", data);
    std::cout << "books_200M_uint32" << std::endl;
    if (print_stats_flag) print_stats(data);

    return 0;
}
