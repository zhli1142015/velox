/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common {

/// Represents a closed interval of row indices [from_, to_].
///
/// PageRowRange is used to describe a contiguous range of row indices,
/// inclusive of both endpoints. It provides utility methods for counting the
/// number of rows, checking relative positions between ranges, and computing
/// unions and intersections.
///
/// Invariants:
/// - from_ <= to_
///
/// Example:
///   PageRowRange r(10, 20); // Represents rows 10 through 20, inclusive.
struct PageRowRange {
  uint64_t from_;
  uint64_t to_;

  PageRowRange(uint64_t from, uint64_t to) : from_(from), to_(to) {
    VELOX_CHECK_LE(from, to);
  }

  uint64_t count() const {
    return to_ - from_ + 1;
  }

  bool isBefore(const PageRowRange& other) const {
    return to_ < other.from_;
  }

  bool isAfter(const PageRowRange& other) const {
    return from_ > other.to_;
  }

  static std::optional<PageRowRange> tryUnion(
      const PageRowRange& left,
      const PageRowRange& right) {
    if (left.from_ <= right.from_) {
      if (left.to_ + 1 >= right.from_) {
        return PageRowRange(left.from_, std::max(left.to_, right.to_));
      }
    } else {
      if (right.to_ + 1 >= left.from_) {
        return PageRowRange(right.from_, std::max(left.to_, right.to_));
      }
    }
    return std::nullopt;
  }

  static std::optional<PageRowRange> intersection(
      const PageRowRange& left,
      const PageRowRange& right) {
    if (left.from_ <= right.from_) {
      if (left.to_ >= right.from_) {
        return PageRowRange(right.from_, std::min(left.to_, right.to_));
      }
    } else {
      if (right.to_ >= left.from_) {
        return PageRowRange(left.from_, std::min(left.to_, right.to_));
      }
    }
    return std::nullopt;
  }

  /// Difference of two RowRanges: left \ right.
  /// Returns at most two disjoint ranges.
  static std::vector<PageRowRange> difference(
      const PageRowRange& left,
      const PageRowRange& right) {
    std::vector<PageRowRange> result;
    auto inter = intersection(left, right);
    if (!inter) {
      // No overlap, entire left remains.
      result.push_back(left);
    } else {
      // Before intersection.
      if (left.from_ < inter->from_) {
        result.emplace_back(left.from_, inter->from_ - 1);
      }
      // After intersection.
      if (inter->to_ < left.to_) {
        result.emplace_back(inter->to_ + 1, left.to_);
      }
    }
    return result;
  }

  std::string toString() const {
    std::ostringstream os;
    os << "[" << from_ << ", " << to_ << "]";
    return os.str();
  }
};

/// Represents a collection of row intervals (ranges) for efficient row
/// selection and manipulation.
class RowRanges {
 public:
  RowRanges() = default;

  explicit RowRanges(const PageRowRange& range) {
    ranges_.push_back(range);
  }

  /// Creates a RowRanges object representing a single continuous range of rows
  /// starting from 0 up to (rowCount - 1).
  static RowRanges createSingle(int64_t rowCount) {
    VELOX_CHECK_GE(rowCount, 0);
    if (rowCount == 0) {
      return RowRanges();
    }
    return RowRanges(PageRowRange(0, rowCount - 1));
  }

  /// Computes the complement of the given RowRanges within the range [0,
  /// maxRow). The complement includes all rows not covered by the input ranges.
  static RowRanges complement(const RowRanges& src, int64_t maxRow) {
    RowRanges result;
    int64_t cursor = 0;
    for (auto& r : src.getRanges()) {
      if (cursor < r.from_) {
        result.add(PageRowRange(cursor, r.from_ - 1));
      }
      cursor = r.to_ + 1;
    }
    if (cursor < maxRow) {
      result.add(PageRowRange(cursor, maxRow - 1));
    }
    return result;
  }

  /// Computes the unionWith of two RowRanges objects.
  static RowRanges unionWith(const RowRanges& left, const RowRanges& right) {
    std::vector<PageRowRange> all = left.ranges_;
    all.insert(all.end(), right.ranges_.begin(), right.ranges_.end());
    std::sort(
        all.begin(),
        all.end(),
        [](const PageRowRange& a, const PageRowRange& b) {
          return a.from_ < b.from_;
        });
    RowRanges result;
    for (const auto& r : all) {
      result.add(r);
    }
    return result;
  }

  /// Computes the intersection of two RowRanges objects.
  static RowRanges intersection(const RowRanges& left, const RowRanges& right) {
    RowRanges result;
    size_t i = 0, j = 0;
    const auto& A = left.ranges_;
    const auto& B = right.ranges_;
    while (i < A.size() && j < B.size()) {
      const PageRowRange& a = A[i];
      const PageRowRange& b = B[j];
      if (a.isAfter(b)) {
        j++;
      } else if (b.isAfter(a)) {
        i++;
      } else {
        auto inter = PageRowRange::intersection(a, b);
        if (inter.has_value()) {
          result.add(*inter);
        }
        if (a.to_ < b.to_) {
          i++;
        } else {
          j++;
        }
      }
    }
    return result;
  }

  /// Computes the unionWith of this RowRanges with another RowRanges.
  void unionWith(const RowRanges& other) {
    *this = unionWith(*this, other);
  }

  /// Computes the intersection of this RowRanges with another RowRanges.
  void intersectWith(const RowRanges& other) {
    *this = intersection(*this, other);
  }

  /// Add an interval to the end, and merge with the previous one if possible.
  void add(const PageRowRange& range) {
    if (ranges_.empty()) {
      ranges_.push_back(range);
      return;
    }
    auto& last = ranges_.back();
    // Try to merge: if overlapping or adjacent, extend the last interval;
    // otherwise, append directly.
    if (auto m = PageRowRange::tryUnion(last, range)) {
      last = *m;
    } else {
      VELOX_CHECK(last.isBefore(range));
      ranges_.push_back(range);
    }
  }

  /// Return the total number of elements in all intervals.
  int64_t rowCount() const {
    int64_t cnt = 0;
    for (const auto& r : ranges_) {
      cnt += r.count();
    }
    return cnt;
  }

  /// Check if the given interval [from, to] overlaps with any existing
  /// interval.
  bool isOverlapping(int64_t from, int64_t to) const {
    PageRowRange query(from, to);
    for (const auto& r : ranges_) {
      if (query.isBefore(r)) {
        break;
      }
      if (!query.isAfter(r)) {
        return true;
      }
    }
    return false;
  }

  /// Return string representation, e.g. "[[0, 9], [15, 20]]".
  std::string toString() const {
    std::ostringstream os;
    os << "[";
    for (size_t i = 0; i < ranges_.size(); ++i) {
      if (i)
        os << ", ";
      os << ranges_[i].toString();
    }
    os << "]";
    return os.str();
  }

  const std::vector<PageRowRange>& getRanges() const {
    return ranges_;
  }

  /// Computes the intersection of this RowRanges with a single PageRowRange.
  /// Returns std::optional<PageRowRange> of the intersected range if any
  /// overlap.
  std::optional<PageRowRange> intersectOne(const PageRowRange& r) const {
    for (const auto& existing : ranges_) {
      if (auto inter = PageRowRange::intersection(existing, r)) {
        return inter;
      }
    }
    return std::nullopt;
  }

  static std::pair<PageRowRange, bool> firstSplitByIntersection(
      const PageRowRange& r,
      const RowRanges& rs) {
    const auto& ranges = rs.getRanges();
    uint64_t cursor = r.from_;
    uint64_t end = r.to_;

    for (const auto& valid : ranges) {
      if (valid.to_ < cursor) {
        continue; // This valid range is entirely before `r`.
      }
      if (valid.from_ > end) {
        break; // No more overlap possible.
      }

      if (cursor < valid.from_) {
        // Non-overlapping prefix before the valid range.
        return {PageRowRange(cursor, std::min(valid.from_ - 1, end)), false};
      }

      // Overlapping segment.
      return {PageRowRange(cursor, std::min(valid.to_, end)), true};
    }

    // No overlap at all.
    return {PageRowRange(cursor, end), false};
  }

  void updateAffectedPages(int64_t affectedPages) {
    affectedPages_ += affectedPages;
  }

  int64_t affectedPages() const {
    return affectedPages_;
  }

  void updateCoveredPages(int64_t coveredPages) {
    coveredPages_ += coveredPages;
  }

  int64_t coveredPages() const {
    return coveredPages_;
  }

 private:
  std::vector<PageRowRange> ranges_;
  int64_t affectedPages_{0}; // Number of pages affected by this RowRanges.
  int64_t coveredPages_{0}; // Number of pages covered by this RowRanges.
};
} // namespace facebook::velox::dwio::common
