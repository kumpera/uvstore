#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <deque>
#include <unordered_map>
#include <algorithm>

#include "uvstore.hpp"

//FIXME don't abort
#define UV_ASSERT(cond, str) do { if(!(cond)) { printf("UvStore fail:%s\n", str); exit(-1); } } while(false)

namespace detail {
class UvServer {
public:
  static std::shared_ptr<UvServer> start(const c10d::TCPStoreOptions& opts);


};


class UvClient {
public:
  static std::unique_ptr<UvClient> connect(const std::string& host, int port);
};

} //namespace detail


UvStore::UvStore(std::string host, const c10d::TCPStoreOptions& opts) : Store(opts.timeout) {
    UV_ASSERT(!opts.multiTenant, "can't do multi tenant"); // FIXME implement this

    if (opts.isServer) {
        server_ = detail::UvServer::start(opts);
    }

    client_ = detail::UvClient::connect(host, opts.port);

  if (opts.waitWorkers) {
    waitForWorkers();
  }

}

void UvStore::waitForWorkers() {
    //TODO
//   if (numWorkers_ == c10::nullopt) {
//     return;
//   }

//   incrementValueBy(initKey_, 1);

//   // Let server block until all workers have completed, this ensures that
//   // the server daemon thread is always running until the very end
//   if (server_) {
//     const auto start = std::chrono::steady_clock::now();
//     while (true) {
//       // TODO: Any chance to make this cleaner?
//       std::vector<uint8_t> value = doGet(initKey_);
//       auto buf = reinterpret_cast<const char*>(value.data());
//       auto len = value.size();
//       int numWorkersCompleted = std::stoi(std::string(buf, len));
//       if (numWorkersCompleted >= static_cast<int>(*numWorkers_)) {
//         break;
//       }
//       const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
//           std::chrono::steady_clock::now() - start);
//       if (timeout_ != kNoTimeout && elapsed > timeout_) {
//         break;
//       }
//       /* sleep override */
//       std::this_thread::sleep_for(std::chrono::milliseconds(10));
//     }
//   }
}


UvStore::~UvStore() = default;

/*
explicit UvStore(std::string host, const c10d::TCPStoreOptions& opts = {});

  ~UvStore() override;

  void set(const std::string& key, const std::vector<uint8_t>& value) override;

  std::vector<uint8_t> compareSet(
      const std::string& key,
      const std::vector<uint8_t>& expectedValue,
      const std::vector<uint8_t>& desiredValue) override;

  std::vector<uint8_t> get(const std::string& key) override;

  int64_t add(const std::string& key, int64_t value) override;

  bool deleteKey(const std::string& key) override;

  int64_t getNumKeys() override;

  bool check(const std::vector<std::string>& keys) override;

  void wait(const std::vector<std::string>& keys) override;

  void wait(
      const std::vector<std::string>& keys,
      const std::chrono::milliseconds& timeout) override;

  const std::chrono::milliseconds& getTimeout() const noexcept override;

  void setTimeout(const std::chrono::milliseconds& timeout) override;

  void watchKey(const std::string& key, c10d::WatchKeyCallback callback) override;

  void append(
      const std::string& key,
      const std::vector<uint8_t>& value) override;

  std::vector<std::vector<uint8_t>> multiGet(const std::vector<std::string>& keys) override;

  void multiSet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint8_t>>& values) override;

  // Returns true if this store support watchKey, append, multiGet and multiSet
  bool hasExtendedApi() const override;*/




void init_uv_store(int rank, int world_size, int port) {
    c10d::TCPStoreOptions opts;
    opts.port = port;
    opts.numWorkers = world_size;
    if (rank == 0) {
        opts.isServer = true;
    }

    UvStore store("127.0.0.1", opts);
}

int main() {
    init_uv_store(0, 0, 24563);   
}