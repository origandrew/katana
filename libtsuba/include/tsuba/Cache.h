#ifndef KATANA_LIBTSUBA_TSUBA_CACHE_H_
#define KATANA_LIBTSUBA_TSUBA_CACHE_H_

// Implements a cache with an LRU replacement policy and a max size replacement
// policy.  Can be configured to call back with an evicted key.

#include <list>
#include <optional>
#include <string>
#include <utility>

#include <arrow/chunked_array.h>

#include "katana/ArrowInterchange.h"
#include "katana/ConcurrentMap.h"
#include "katana/Logging.h"

namespace tsuba {

struct KATANA_EXPORT KeyBase {
  enum class NodeEdge { kNode, kEdge };
  bool operator==(const KeyBase& o) const {
    return node_edge_ == o.node_edge_ && name_ == o.name_;
  }

  // This was supposed to suffice, but only the std::hash worked.
  //friend size_t hash_value(const KeyBase& key)

  NodeEdge node_edge_;
  std::string name_;
};

}  // namespace tsuba
namespace std {
// inject specialization of std::hash for KeyBase into namespace std
// ----------------------------------------------------------------
template <>
struct hash<tsuba::KeyBase> {
  std::size_t operator()(tsuba::KeyBase const& key) const {
    return phmap::HashState().combine(0, key.node_edge_, key.name_);
  }
};
}  // namespace std

namespace tsuba {
template <typename Value>
class KATANA_EXPORT Cache {
public:
  using ListType = std::list<KeyBase>;
  using MapValue = std::pair<Value, typename ListType::iterator>;
  using MapType = katana::ConcurrentMap<KeyBase, MapValue>;
  enum class ReplacementPolicy { kLRU, kSize };

  Cache(
      ReplacementPolicy policy = ReplacementPolicy::kLRU,
      size_t lru_capacity = 0, size_t byte_capacity = 0,
      std::function<void(const KeyBase& key)> evict_cb = nullptr,
      std::function<size_t(const Value& value)> value_to_bytes = nullptr)
      : policy_(policy),
        lru_capacity_(lru_capacity),
        byte_capacity_(byte_capacity),
        evict_cb_(std::move(evict_cb)),
        value_to_bytes_(std::move(value_to_bytes)) {
    KATANA_LOG_VASSERT(
        policy_ != ReplacementPolicy::kLRU || lru_capacity_ > 0,
        "kSize policy requires non-zero LRU entries");
    KATANA_LOG_VASSERT(
        policy_ != ReplacementPolicy::kSize || byte_capacity_ > 0,
        "kSize policy requires non-zero byte capacity");
  }

  size_t size() const { return key_to_value_.size(); }
  size_t bytes() const { return total_bytes_; }

  size_t LRUCapacity() const { return lru_capacity_; }
  size_t ByteCapacity() const { return byte_capacity_; }

  bool Empty() const { return key_to_value_.empty(); }

  bool Contains(const KeyBase& key) {
    return key_to_value_.find(key) != key_to_value_.end();
  }

  void Insert(const KeyBase& key, const Value& value) {
    list_lock.lock();
    lru_list_.push_front(key);
    key_to_value_.insert_or_assign(key, {value, lru_list_.begin()});
    if (value_to_bytes_ != nullptr) {
      total_bytes_ += value_to_bytes_(value);
    }
    // List unlock after reading begin() and the map insert
    list_lock.unlock();
    evict_if_necessary();
  }

  std::optional<Value> Get(const KeyBase& key) {
    // lookup value in the cache
    std::optional<Value> ret = std::nullopt;
    key_to_value_.modify_if(key, [&](MapValue& value) {
      auto it = value.second;
      list_lock.lock();
      auto head = lru_list_.begin();
      if (it != head) {
        // move item to the front of the most recently used list
        lru_list_.splice(head, lru_list_, it);
        list_lock.unlock();
        // update iterator in map value
        value = {value.first, head};
      } else {
        list_lock.unlock();
      }
      // return the value
      ret = value.first;
    });
    return ret;
  }

private:
  void evict_if_necessary() {
    switch (policy_) {
    case ReplacementPolicy::kLRU: {
      while (size() > lru_capacity_) {
        KeyBase evicted_key;
        // evict item from the end of most recently used list
        list_lock.lock();
        auto tail = --lru_list_.end();
        KATANA_LOG_ASSERT(tail != lru_list_.end());
        evicted_key = *tail;
        if (value_to_bytes_ != nullptr) {
          key_to_value_.if_contains(evicted_key, [&](MapValue value) {
            total_bytes_ -= value_to_bytes_(value.first);
          });
        }
        key_to_value_.erase(*tail);
        lru_list_.erase(tail);
        list_lock.unlock();
        if (evict_cb_) {
          evict_cb_(evicted_key);
        }
      }
    } break;
    case ReplacementPolicy::kSize: {
      KATANA_LOG_DEBUG_ASSERT(value_to_bytes_ != nullptr);
      // Allow a single entry to exceed our byte capacity
      while (bytes() > byte_capacity_ && size() > 1) {
        KeyBase evicted_key;
        // evict item from the end of most recently used list
        list_lock.lock();
        auto tail = --lru_list_.end();
        KATANA_LOG_ASSERT(tail != lru_list_.end());
        evicted_key = *tail;
        key_to_value_.if_contains(evicted_key, [&](MapValue value) {
          total_bytes_ -= value_to_bytes_(value.first);
        });
        key_to_value_.erase(*tail);
        lru_list_.erase(tail);
        list_lock.unlock();
        if (evict_cb_) {
          evict_cb_(evicted_key);
        }
      }
    } break;
    default:
      KATANA_LOG_FATAL("bad cache replacement policy: {}", policy_);
    }
  }

  // Concurrent map
  MapType key_to_value_;
  // Protects the list and total_bytes_
  katana::SimpleLock list_lock;
  // List and its single, global lock
  // TODO (witchel) do we need more concurrency?
  ListType lru_list_;
  size_t total_bytes_{0};

  ReplacementPolicy policy_;
  std::atomic<size_t> lru_capacity_{0};   // for kLRU
  std::atomic<size_t> byte_capacity_{0};  // for kSize
  std::function<void(const KeyBase& key)> evict_cb_;
  std::function<size_t(const Value& value)> value_to_bytes_;
};

using PropertyCache = Cache<std::shared_ptr<arrow::ChunkedArray>>;

}  // namespace tsuba

#endif
