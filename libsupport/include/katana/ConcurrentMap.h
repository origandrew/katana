#ifndef KATANA_LIBSUPPORT_KATANA_CONCURRENTMAP_H_
#define KATANA_LIBSUPPORT_KATANA_CONCURRENTMAP_H_

#include <parallel_hashmap/phmap.h>

#include "katana/SimpleLock.h"

namespace katana {

/// Specialized parallel_flat_hash_map type that makes use of galois's
/// SimpleLock (spinlock)
///
/// The implementation will allocate 2**N sub maps, each sub map is
/// locked independently. The choice of 4 (so 16 hashtables) is the
/// library's default.
template <class K, class V, size_t N = 9>
using ConcurrentMap = phmap::parallel_flat_hash_map<
    K, V, std::hash<K>, std::equal_to<K>, std::allocator<std::pair<K, V>>, N,
    SimpleLock>;

template <class T, size_t N = 9>
using ConcurrentSet = phmap::parallel_flat_hash_set<
    T, std::hash<T>, std::equal_to<T>, std::allocator<T>, N, SimpleLock>;

}  // namespace katana

#endif
