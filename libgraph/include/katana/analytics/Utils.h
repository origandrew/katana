#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_UTILS_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_UTILS_H_

#include <algorithm>
#include <random>
#include <utility>

#include <arrow/type.h>

#include "arrow/util/bitmap.h"
#include "katana/ErrorCode.h"
#include "katana/PropertyGraph.h"
#include "katana/Result.h"

namespace katana::analytics {

// TODO(amp): This file should be disbanded and its functions moved to
// PropertyGraph.h or other more specific places.

//! Used to pick random non-zero degree starting points for search algorithms
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/benchmark.h)
class KATANA_EXPORT SourcePicker {
  const PropertyGraph& graph;

public:
  explicit SourcePicker(const PropertyGraph& g) : graph(g) {}

  uint32_t PickNext();
};

//! Used to determine if a graph has power-law degree distribution or not
//! by sampling some of the vertices in the graph randomly
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/tc.cc WorthRelabelling())
KATANA_EXPORT bool IsApproximateDegreeDistributionPowerLaw(
    const PropertyGraph& graph);

class KATANA_EXPORT TemporaryPropertyGuard {
  static thread_local int temporary_property_counter;

  std::optional<katana::PropertyGraph::MutablePropertyView> property_view_ =
      std::nullopt;
  std::string name_;
  std::unique_ptr<katana::TxnContext> txn_ctx_;  // Temporary TxnContext

  std::string GetPropertyName() {
    // Use a thread local counter and the thread ID to get a unique name.
    // `this` is not unique because we support moves.
    return fmt::format(
        "__katana_temporary_property_{}_{}", std::this_thread::get_id(),
        temporary_property_counter++);
  }

  void Deinit() {
    if (!property_view_) {
      return;
    }

    // Since the property is a temporary, thread-local one, we don't need
    // to pass the TxnContext to the caller. Hence, use a local TxnContext.
    if (auto r = property_view_->RemoveProperty(name_, txn_ctx_.get()); !r) {
      if (r.error() != ErrorCode::PropertyNotFound) {
        // Log an error if something goes wrong other than the property not
        // existing.
        KATANA_LOG_WARN("Failed to remove temporary property: {}", r.error());
      }
    }
    Clear();
  }

  void Clear() {
    property_view_ = std::nullopt;
    txn_ctx_.reset();
  }

public:
  TemporaryPropertyGuard() = default;

  TemporaryPropertyGuard(PropertyGraph* pv)
      : TemporaryPropertyGuard(pv->NodeMutablePropertyView()) {}

  explicit TemporaryPropertyGuard(PropertyGraph::MutablePropertyView pv)
      : property_view_(pv) {
    name_ = GetPropertyName();
    txn_ctx_ = std::make_unique<katana::TxnContext>();
  }

  const TemporaryPropertyGuard& operator=(const TemporaryPropertyGuard&) =
      delete;
  TemporaryPropertyGuard(const TemporaryPropertyGuard&) = delete;

  TemporaryPropertyGuard(TemporaryPropertyGuard&& rhs) noexcept
      : property_view_(rhs.property_view_), name_(std::move(rhs.name_)) {
    txn_ctx_ = std::make_unique<katana::TxnContext>();
    rhs.Clear();
  }

  TemporaryPropertyGuard& operator=(TemporaryPropertyGuard&& rhs) noexcept {
    Deinit();
    property_view_ = rhs.property_view_;
    name_ = std::move(rhs.name_);
    txn_ctx_ = std::make_unique<katana::TxnContext>();
    rhs.Clear();
    return *this;
  }

  std::string name() const { return name_; }

  ~TemporaryPropertyGuard() { Deinit(); }
};

}  // namespace katana::analytics

#endif
