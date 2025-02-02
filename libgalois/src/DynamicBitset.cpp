/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2019, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

/**
 * @file DynamicBitset.cpp
 */

#include "katana/DynamicBitset.h"

#include "katana/Galois.h"

KATANA_EXPORT katana::DynamicBitset katana::EmptyBitset;

void
katana::DynamicBitset::bitwise_or(const DynamicBitset& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] |= other_bitvec[i]; }, katana::no_stats());
}

void
katana::DynamicBitset::bitwise_not() {
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] = ~bitvec_[i]; }, katana::no_stats());
}

void
katana::DynamicBitset::bitwise_and(const DynamicBitset& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] &= other_bitvec[i]; }, katana::no_stats());
}

void
katana::DynamicBitset::bitwise_and(
    const DynamicBitset& other1, const DynamicBitset& other2) {
  KATANA_LOG_DEBUG_ASSERT(size() == other1.size());
  KATANA_LOG_DEBUG_ASSERT(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] = other_bitvec1[i] & other_bitvec2[i]; },
      katana::no_stats());
}

void
katana::DynamicBitset::bitwise_xor(const DynamicBitset& other) {
  KATANA_LOG_DEBUG_ASSERT(size() == other.size());
  const auto& other_bitvec = other.get_vec();
  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] ^= other_bitvec[i]; }, katana::no_stats());
}

void
katana::DynamicBitset::bitwise_xor(
    const DynamicBitset& other1, const DynamicBitset& other2) {
  KATANA_LOG_DEBUG_ASSERT(size() == other1.size());
  KATANA_LOG_DEBUG_ASSERT(size() == other2.size());
  const auto& other_bitvec1 = other1.get_vec();
  const auto& other_bitvec2 = other2.get_vec();

  katana::do_all(
      katana::iterate(size_t{0}, bitvec_.size()),
      [&](size_t i) { bitvec_[i] = other_bitvec1[i] ^ other_bitvec2[i]; },
      katana::no_stats());
}

size_t
katana::DynamicBitset::count() const {
  katana::GAccumulator<size_t> ret;
  katana::do_all(
      katana::iterate(bitvec_.begin(), bitvec_.end()),
      [&](uint64_t n) {
#ifdef __GNUC__
        ret += __builtin_popcountll(n);
#else
        n = n - ((n >> 1) & 0x5555555555555555UL);
        n = (n & 0x3333333333333333UL) + ((n >> 2) & 0x3333333333333333UL);
        ret += (((n + (n >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >>
               56;
#endif
      },
      katana::no_stats());
  return ret.reduce();
}

size_t
katana::DynamicBitset::SerialCount() const {
  size_t ret = 0;
  for (uint64_t n : bitvec_) {
#ifdef __GNUC__
    ret += __builtin_popcountll(n);
#else
    n = n - ((n >> 1) & 0x5555555555555555UL);
    n = (n & 0x3333333333333333UL) + ((n >> 2) & 0x3333333333333333UL);
    ret += (((n + (n >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56;
#endif
  }
  return ret;
}

namespace {
template <typename Integer>
void
ComputeOffsets(
    const katana::DynamicBitset& bitset, std::vector<Integer>* offsets) {
  // TODO uint32_t is somewhat dangerous; change in the future
  uint32_t activeThreads = katana::getActiveThreads();
  std::vector<Integer> tPrefixBitCounts(activeThreads);

  // count how many bits are set on each thread
  katana::on_each([&](unsigned tid, unsigned nthreads) {
    auto [start, end] =
        katana::block_range(size_t{0}, bitset.size(), tid, nthreads);

    Integer count = 0;
    for (Integer i = start; i < end; ++i) {
      if (bitset.test(i)) {
        ++count;
      }
    }

    tPrefixBitCounts[tid] = count;
  });

  // calculate prefix sum of bits per thread
  for (uint32_t i = 1; i < activeThreads; ++i) {
    tPrefixBitCounts[i] += tPrefixBitCounts[i - 1];
  }

  // total num of set bits
  Integer bitsetCount = tPrefixBitCounts[activeThreads - 1];

  // calculate the indices of the set bits and save them to the offset
  // vector
  if (bitsetCount > 0) {
    size_t cur_size = offsets->size();
    offsets->resize(cur_size + bitsetCount);
    katana::on_each([&](unsigned tid, unsigned nthreads) {
      auto [start, end] =
          katana::block_range(size_t{0}, bitset.size(), tid, nthreads);
      Integer index = cur_size;
      if (tid != 0) {
        index += tPrefixBitCounts[tid - 1];
      }

      for (Integer i = start; i < end; ++i) {
        if (bitset.test(i)) {
          offsets->at(index) = i;
          ++index;
        }
      }
    });
  }
}
}  //namespace

template <>
std::vector<uint32_t>
katana::DynamicBitset::GetOffsets<uint32_t>() const {
  std::vector<uint32_t> offsets;
  ComputeOffsets<uint32_t>(*this, &offsets);
  return offsets;
}

template <>
std::vector<uint64_t>
katana::DynamicBitset::GetOffsets<uint64_t>() const {
  std::vector<uint64_t> offsets;
  ComputeOffsets<uint64_t>(*this, &offsets);
  return offsets;
}

template <>
void
katana::DynamicBitset::AppendOffsets(std::vector<uint32_t>* offsets) const {
  ComputeOffsets<uint32_t>(*this, offsets);
}

template <>
void
katana::DynamicBitset::AppendOffsets(std::vector<uint64_t>* offsets) const {
  ComputeOffsets<uint64_t>(*this, offsets);
}
