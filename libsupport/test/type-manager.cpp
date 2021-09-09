#include "katana/EntityTypeManager.h"
#include "katana/Logging.h"

int
main() {
  std::vector<katana::TypeNameSet> tnss = {
      {"alice"},
      {"baker"},
      {"alice", "baker"},
      {"charlie"},
      {"david", "eleanor"}};
  std::vector<katana::TypeNameSet> check = {
      {std::string(katana::kUnknownEntityTypeName)},
      {"alice"},
      {"baker"},
      {"alice", "baker"},
      {"charlie"},
      {"david"},
      {"eleanor"},
      {"david", "eleanor"}};
  katana::EntityTypeManager mgr;
  for (const auto& tns : tnss) {
    auto res = mgr.GetOrAddNonAtomicEntityTypeFromStrings(tns);
    KATANA_LOG_ASSERT(res);
  }
  auto num_entities = mgr.GetNumEntityTypes();
  for (size_t i = 0; i < mgr.GetNumEntityTypes(); ++i) {
    auto res = mgr.EntityTypeToTypeNameSet(i);
    KATANA_LOG_ASSERT(res);
    auto tns = res.value();
    KATANA_LOG_VASSERT(
        tns == check[i], "i={} tns ({}) tnss[i-1] ({}", i, tns, check[i]);
  }
  katana::TypeNameSet new_tns({"new", "one"});
  auto res = mgr.GetOrAddNonAtomicEntityTypeFromStrings(new_tns);
  KATANA_LOG_ASSERT(res);
  KATANA_LOG_ASSERT(res.value() >= num_entities);
  num_entities = mgr.GetNumEntityTypes();

  // Can't ask for empty set
  katana::TypeNameSet empty;
  res = mgr.GetNonAtomicEntityTypeFromStrings(empty);
  KATANA_LOG_ASSERT(!res);
  fmt::print("{}", mgr.PrintTypes());
}
