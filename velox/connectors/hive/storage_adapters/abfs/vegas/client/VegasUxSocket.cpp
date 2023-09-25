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

#include "VegasUxSocket.h"
#include <asm-generic/ioctls.h>
#include <glog/logging.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>

namespace facebook::velox::filesystems::abfs::vegas {

VegasUxSocket::VegasUxSocket() = default;

bool VegasUxSocket::Write(const void* src, long bytesToWrite) {
  auto address = (std::byte*)src;
  while (bytesToWrite) {
    int res = ::write(fd_, address, bytesToWrite);

    if (res > 0) {
      address += res;
      bytesToWrite -= res;
      totalWritten_ += res;
    } else if (res < 0) {
      if (errno == EAGAIN)
        continue;

      // Failed to write the event. Close the socket and
      // we will retry later.
      //
      std::string strError = strerror(errno);
      LOG(INFO) << "VegasUxSocket.Write.Fail:" << strError;
      return false;
    }
  }
  return true;
}

long VegasUxSocket::Read(const void* buf, long sz) {
  if (Available() < 0)
    return -1;
  auto totalBytes = 0L;
  auto ptr = (std::byte*)buf;
  while (totalBytes < sz) {
    auto res = ::recv(fd_, ptr, sz - totalBytes, MSG_WAITFORONE);

    if (res > 0) {
      totalBytes += res;
      ptr += res;
      readPhysicalPosition_ += res;
      continue;
    }

    if (res <= 0) {
      if (AwaitReadable(15000)) {
        continue;
      }
      return totalBytes > 0 ? totalBytes : -1;
    }
  }
  return totalBytes;
}

long VegasUxSocket::Available() const {
  int count = 0;
  if (int res = ioctl(fd_, FIONREAD, &count); res < 0) {
    // CSysLog::LogInfo("CUxSocketBase.Available.Failed"s + strerror(errno));
    return -1;
  }
  return count;
}

bool VegasUxSocket::Connect(const std::string& name) {
  struct sockaddr_un server;

  // This would only fail on out-of-memory type situations.
  //
  fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd_ == -1) {
    return false;
  }

  server.sun_family = AF_UNIX;
  strcpy(server.sun_path, name.c_str());
  std::string errMessage;

  for (;;) {
    int res =
        connect(fd_, (struct sockaddr*)&server, sizeof(struct sockaddr_un));

    if (res == 0 || res == EINPROGRESS) {
      if (IsConnected() && AwaitWritable()) {
        name_ = name;
        return true;
      }
    }

    errMessage = strerror(errno);

    if (res == EAGAIN) {
      continue;
    }

    break;
  }

  Close();
  return false;
}

void VegasUxSocket::Close() {
  if (fd_ != -1) {
    close(fd_);
  }
  name_.clear();
  fd_ = -1;
}

bool VegasUxSocket::AwaitReadable(int msec) {
  // The following sequence doesn't seem obvious, but follows the docs.
  // str
  // First, poll until socket is writeable
  //
  pollfd pfd;
  ::memset(&pfd, 0, sizeof(pollfd));
  pfd.events = POLLIN;
  pfd.fd = fd_;
  int res = ::poll(&pfd, 1, msec);
  if (res == 0)
    return false;

  if ((pfd.revents & POLLIN) == 0) {
    return false;
  }
  return true;
}

bool VegasUxSocket::AwaitWritable(int msec) {
  // The following sequence doesn't seem obvious, but follows the docs.
  //
  // First, poll until socket is writeable
  //
  pollfd pfd;
  ::memset(&pfd, 0, sizeof(pollfd));
  pfd.events = POLLOUT;
  pfd.fd = fd_;
  int res = ::poll(&pfd, 1, msec);
  if (res == 0)
    return false;

  if ((pfd.revents & POLLOUT) == 0) {
    return false;
  }
  return true;
}
} // namespace facebook::velox::filesystems::abfs::vegas
