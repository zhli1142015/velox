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

#include <fcntl.h>
#include <semaphore.h>
#include <sys/stat.h>
#include <memory>
#include <mutex>
#include <string>

#include "folly/String.h"
#include "glog/logging.h"

namespace facebook::velox::filesystems::abfs::vegas {
class VegasSemaphore {
 public:
  // Name must have an initial slash, followed by non-slash characters
  // Examples
  //    /somename
  //    /vegas.sem.12341a123-123
  //
  // The max name is 251 characters
  //
  VegasSemaphore(const std::string& name) : name_(name) {}

  VegasSemaphore();

  VegasSemaphore(const VegasSemaphore&) = delete;
  VegasSemaphore& operator=(const VegasSemaphore&) = delete;

  ~VegasSemaphore() {
    if (psem_ != nullptr) {
      psem_ = nullptr;
    }
  }

  bool open(int count = 1) {
    int flags = O_CREAT;
    int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

    auto res = sem_open(name_.c_str(), flags, mode, count);

    if (res == SEM_FAILED) {
      VLOG(1) << "Failed to open semaphore: " << name_
              << " errno: " << folly::errnoStr(errno);
      return false;
    }
    psem_ = res;
    VLOG(1) << "Semaphore " << name_ << " getValue: " << getValue();
    return true;
  }

  void close() {
    if (psem_) {
      if (sem_close(psem_) == -1) {
        VLOG(1) << "Failed to close semaphore: " << name_
                << " errno: " << folly::errnoStr(errno);
      }
      psem_ = nullptr;
    }
  }

  int getValue() {
    int value;
    if (psem_ && sem_getvalue(psem_, &value) == 0) {
      return value;
    } else {
      return -1;
    }
  }

  void unlink() {
    if (sem_unlink(name_.c_str()) == -1) {
      VLOG(1) << "Failed to unlink semaphore: " << name_
              << " errno: " << folly::errnoStr(errno);
    }
  }

  bool tryWait() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!psem_) {
      init = false;
    }
    if (sem_trywait(psem_) == 0) {
      VLOG(1) << "Semaphore acquired: " << name_;
      auto res = kSemaphoreRefCount.insert({name_, 1});
      init = res.second;
    } else {
      auto it = kSemaphoreRefCount.find(name_);
      if (it != kSemaphoreRefCount.end()) {
        it->second++;
        VLOG(1) << "Semaphore incremented: " << name_
                << " count: " << it->second;
        init = true;
      } else {
        init = false;
      }
    }
    return init;
  }

  bool release() {
    if (init) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = kSemaphoreRefCount.find(name_);
      if (it != kSemaphoreRefCount.end()) {
        it->second--;
        VLOG(1) << "Semaphore " << name_
                << " decremented, count: " << it->second;
        if (it->second == 0) {
          if (sem_post(psem_) == -1) {
            it->second++;
            return false;
          } else {
            kSemaphoreRefCount.erase(name_);
            VLOG(1) << "Semaphore " << name_ << " removed from map";
            close();
            unlink();
            VLOG(1) << "Semaphore " << name_ << " unlinked";
          }
        } else {
          close();
        }
      }
    }
    return true;
  }

 private:
  sem_t* psem_ = nullptr;
  bool init = false;
  std::string name_{""};
  static std::unordered_map<std::string, uint64_t> kSemaphoreRefCount;
  static std::mutex mutex_;
};
} // namespace facebook::velox::filesystems::abfs::vegas
