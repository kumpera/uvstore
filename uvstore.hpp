#pragma once
#include <torch/csrc/distributed/c10d/Store.hpp>
#include <torch/csrc/distributed/c10d/TCPStore.hpp>

namespace detail {
class UvServer;
class UvClient;
}

class UvStore : c10d::Store {
  std::shared_ptr<detail::UvServer> server_;
  std::unique_ptr<detail::UvClient> client_;

    void waitForWorkers();
public:
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
  bool hasExtendedApi() const override;
};
