#pragma once

enum Workload : int{
  LOOKUP_EXISTING,
  LOOKUP_IN_DISTRIBUTION,
  INSERT_IN_DISTRIBUTION,
  
  NUM_WORKLOADS,  // TODO: the ones below are currently disabled
  MIXED,
  SHIFTING,
};

std::string workload_name(Workload workload) {
  switch (workload) {
    case LOOKUP_EXISTING: return "LOOKUP_EXISTING";
    case LOOKUP_IN_DISTRIBUTION: return "LOOKUP_IN_DISTRIBUTION";
    case INSERT_IN_DISTRIBUTION: return "INSERT_IN_DISTRIBUTION";
    case MIXED: return "MIXED";
    case SHIFTING: return "SHIFTING";
    default: return "UNKNOWN_WORKLOAD";
  }
}
