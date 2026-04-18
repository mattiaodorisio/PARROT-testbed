#pragma once

/**
 *
 * Adapted from
 * https://stackoverflow.com/questions/9983239/how-to-generate-zipf-distributed-numbers-efficiently
 *
 * NOTE: this function has a memory leak due to the static allocation of sum_probs,
 * but since this programs ends immediatly after calling it it is not a problem in practice.
 */
template <class ForwardIt>
void zipf_distr(ForwardIt first_, const ForwardIt last_, double skew = 0.75,
                     size_t cardinality = 1e8) {
  typedef typename std::iterator_traits<ForwardIt>::value_type T;
  const size_t size = std::distance(first_, last_);

  srand(42);

  // Start generating numbers
  for (size_t i = 0; i < size; ++i) {
    static bool first = true;  // Static first time flag
    static double c = 0;       // Normalization constant
    static double *sum_probs;  // Pre-calculated sum of probabilities

    // Compute normalization constant on first call only
    if (first) {
      for (size_t i = 1; i <= cardinality; ++i)
        c = c + (1.0 / pow((double)i, skew));
      c = 1.0 / c;

      sum_probs = (double *)malloc((cardinality + 1) * sizeof(*sum_probs));
      sum_probs[0] = 0;
      for (size_t i = 1; i <= cardinality; ++i) {
        sum_probs[i] = sum_probs[i - 1] + c / pow((double)i, skew);
      }
      first = false;
    }

    // Pull a uniform random number (0 < z < 1)
    double z;
    do {
      z = 1. * rand() / RAND_MAX;
    } while ((z == 0) || (z == 1));

    // Map z to the value
    size_t low = 1, high = cardinality, mid;
    T zipf_value = 0;  // Computed exponential value to be returned
    do {
      mid = floor((low + high) / 2);

      if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {
        zipf_value = mid;
        break;
      } else if (sum_probs[mid] >= z) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    } while (low <= high);

    first_[i] = zipf_value;
  }

  // Scale values to fit in the range of T
  if (cardinality < std::numeric_limits<T>::max()) {
    double scale = static_cast<double>(std::numeric_limits<T>::max()) / cardinality;
    for (ForwardIt it = first_; it != last_; ++it) {
      *it = static_cast<T>(*it * scale);
    }
  }
}
