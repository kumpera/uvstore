#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <deque>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <thread>
#include "uvstore.hpp"

//FIXME don't abort
#define UV_ASSERT(cond, str) do { if(!(cond)) { printf("UvStore fail:%s\n", str); exit(-1); } } while(false)

#define DEFAULT_BACKLOG 2048
namespace detail {

#define FROM_FIELD(TYPE, FIELD, VALUE) \
    (*reinterpret_cast<TYPE*>(reinterpret_cast<char*>(VALUE) - offsetof(TYPE, FIELD)))


void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    // buf->base = (char*) malloc(MIN(suggested_size, 512));
    buf->base = (char*) malloc(16); //debug value
    buf->len = 16;
    printf("alloc %p\n", buf->base);
}



class UvSrvClient {
    uv_tcp_t client;


    static void read_callback(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
        printf("read_callback!\n");
    }

    static UvSrvClient& from_handle(uv_handle_t *handle) {
        return FROM_FIELD(UvSrvClient, client, handle);
    }

    static void on_close(uv_handle_t* handle) {
        //TODO (unregister)
        //XXX maybe we use the trick of keeping a shared_ptr to itself and dropping it here
        //XXX we got to callback to server to unregister this guy
    }

public:
    UvSrvClient(uv_loop_t *loop) {
        uv_tcp_init(loop, &client);
    }

    uv_stream_t* stream() {
        return (uv_stream_t*)&client;
    }

    void beginRead() {
        uv_read_start((uv_stream_t*) stream(), alloc_buffer, read_callback);
    }

    // template<typename T>
    void close() {
        // done_ = std::move(callback);
        uv_close((uv_handle_t*)&client, on_close);
    }
};

/*
TODO:
    try to bind to ipv6 before ipv4.
    add error handling to all uv_ calls.
    Replace printf calls with proper logging
*/
class UvServer {
    uv_tcp_t server_;
    uv_loop_t loop_;    
    std::thread thread_{};
    std::unordered_set<std::shared_ptr<UvSrvClient>> clients_;


    static UvServer& from_uv_stream(uv_stream_t *stream) {
        return FROM_FIELD(UvServer, server_, stream);
    }

    static void on_new_connection(uv_stream_t *server, int status) {
        from_uv_stream(server).onConnect(status);
    }

    void run_loop() {
        printf("tcpserver daemon running\n");
    }

    void onConnect(int status) {
        if (status < 0) { 
            printf("Accept error: %s\n", uv_strerror(status));
            return;
        }

        auto client = std::make_shared<UvSrvClient>(loop());

        // printf("accepting %p\n", client->get());
        if (!uv_accept((uv_stream_t*)&server_, client->stream())) {
            client->beginRead();
            clients_.emplace(client);
        } else {
            client->close();
        }
    }

public:
  static std::shared_ptr<UvServer> start(const c10d::TCPStoreOptions& opts) {
    return std::make_shared<UvServer>(opts.port);
  }

    UvServer(int port) {
        uv_loop_init(&loop_);
        uv_tcp_init(loop(), &server_);

        // We sync bind to make failures easy
        printf("init with port %d\n", port);
        struct sockaddr_in addr;
        uv_ip4_addr("0.0.0.0", port, &addr);
        uv_tcp_bind(&server_, (const struct sockaddr*)&addr, 0);
        int res = uv_listen((uv_stream_t*)&server_, DEFAULT_BACKLOG, UvServer::on_new_connection);
        UV_ASSERT(!res, uv_strerror(res));
        thread_ = std::thread{&UvServer::run_loop, this};
    }

    uv_loop_t* loop() { return &loop_; }
};


class UvClient {
    std::mutex activeOpLock_;
    const std::string keyPrefix_ = "/";
public:
    static std::unique_ptr<UvClient> connect(const std::string& host, int port) {
        return std::make_unique<UvClient>();
    }

    void set(const std::string& key, const std::vector<uint8_t>& value) {
        // const std::lock_guard<std::mutex> lock(activeOpLock_);
        // client_->set(key, value);
    }

    std::vector<uint8_t> get(const std::string& key) {
        // const std::lock_guard<std::mutex> lock(activeOpLock_);
        // return client_->get(key, value);
        return std::vector<uint8_t>{};
    }

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

void UvStore::set(const std::string& key, const std::vector<uint8_t>& value) {
    client_->set(key, value);
}

std::vector<uint8_t> UvStore::compareSet(
      const std::string& key,
      const std::vector<uint8_t>& expectedValue,
      const std::vector<uint8_t>& desiredValue) {
    UV_ASSERT(false, "compareSet");       
}

std::vector<uint8_t> UvStore::get(const std::string& key) {
    return client_->get(key);
}

int64_t UvStore::add(const std::string& key, int64_t value) {
    UV_ASSERT(false, "add");       
}

bool UvStore::deleteKey(const std::string& key) {
    UV_ASSERT(false, "delete");       
}

int64_t UvStore::getNumKeys() {
    UV_ASSERT(false, "getNumKey");       
}

bool UvStore::check(const std::vector<std::string>& keys) {
    UV_ASSERT(false, "check");       
}

void UvStore::wait(const std::vector<std::string>& keys) {
    UV_ASSERT(false, "wait");       
}

void UvStore::wait(
      const std::vector<std::string>& keys,
      const std::chrono::milliseconds& timeout) {
    UV_ASSERT(false, "wait2");       
}


void UvStore::watchKey(const std::string& key, c10d::WatchKeyCallback callback) {
    UV_ASSERT(false, "watchkey");       
}

void UvStore::append(
      const std::string& key,
      const std::vector<uint8_t>& value) {
    UV_ASSERT(false, "append");       
}

std::vector<std::vector<uint8_t>> UvStore::multiGet(const std::vector<std::string>& keys) {
    UV_ASSERT(false, "multiGet");       
}

void UvStore::multiSet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint8_t>>& values) {
    UV_ASSERT(false, "multiSet");       
}

  bool UvStore::hasExtendedApi() const {
    UV_ASSERT(false, "hasExtendedApi");       
}

void init_uv_store(int rank, int world_size, int port) {
    c10d::TCPStoreOptions opts;
    opts.port = port;
    opts.numWorkers = world_size;
    if (rank == 0) {
        opts.isServer = true;
    }

    c10d::Store* store = new UvStore("127.0.0.1", opts);

    store->set("foo", "bar");
    auto res = store->get("foo");
    printf("got %s\n", res.data());
}

int main() {
    init_uv_store(0, 1, 29501);   
}