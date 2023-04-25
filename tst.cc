#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <string>
#include <vector>
#include <deque>
#include <unordered_map>
#include <algorithm>

#define PORT 1234
#define DEFAULT_BACKLOG 128

enum class QueryType : uint8_t {
  SET,
  COMPARE_SET,
  GET,
  ADD,
  CHECK,
  WAIT,
  GETNUMKEYS,
  WATCH_KEY,
  DELETE_KEY,
};

enum class CheckResponseType : uint8_t { READY, NOT_READY };

enum class WaitResponseType : uint8_t { STOP_WAITING };

enum class WatchResponseType : uint8_t {
  KEY_UPDATED,
  KEY_CREATED,
  KEY_DELETED,
  KEY_CALLBACK_REGISTERED
};

class UvStream {
    std::deque<uv_buf_t> buffers;
    int buff_idx;
    int buff_offset;
    int buff_idx_commit;
    int buff_offset_commit;

public:
    UvStream(): buff_idx(0), buff_offset(0), buff_offset_commit(0) {

    }

    void append(uv_buf_t buf) {
        if(buf.len == 0) {
            free(buf.base);
        } else {
            buffers.push_back(buf);
        }
    }

    bool read1(uint8_t &byte) {
        while(true) {
            if(buff_idx >= buffers.size())
                return false;
            if(buff_offset >= buffers[buff_idx].len) {
                buff_offset = 0;
                ++buff_idx;
                continue;
            }
            break;
        }

        byte = buffers[buff_idx].base[buff_offset];
        ++buff_offset;
        return true;
    }

    template <typename T>
    bool read_value(T& value) {
        uint8_t *val = (uint8_t*)&value;
        //TODO optimize this to read larger chunks from the current buf
        for(int i = 0; i < sizeof(T); ++i) {
            if(!read1(val[i]))
                return false;
        }
        return true;
    }

    bool read_str(std::string &str) {
        uint64_t size = 0;
        if(!read_value(size))
            return false;
        //TODO add and use a isAvailable(size_t sz) that test if at least sz is available
        //TODO avoid allocating this temp vector
        std::vector<char> value(size);
        //TODO optimize this with larger chunks copies
        for(int i = 0; i < size; ++i) {
            if(!read_value(value[i]))
                return false;
        }
        str = std::string(value.data(), value.size());
        return true;
    }

    template <typename T>
    bool read_vector(std::vector<T> &data) {
        uint64_t size = 0;
        if(!read_value(size))
            return false;
        //TODO add and use a isAvailable(size_t sz) that test if at least sz is available
        data.reserve(size);
        for(int i = 0; i < size; ++i) {
            T tmp = {};
            if(!read_value(tmp))
                return false;
            data.push_back(tmp);
        }
        return true;
    }

    void commit () {
        printf("commiting %d [%d] bs:%zu bisz:%zu\n", buff_idx, buff_offset, buffers.size(), buffers[buff_idx].len);
        if(buff_idx >= buffers.size() || buff_offset >= buffers[buff_idx].len) {
            printf("\treset offset, full use\n");
            buff_offset = 0;
            ++buff_idx;
        }

        for(int i = 0; i < buff_idx; ++i) {
            printf("deleting [%d] %p\n", i, buffers[0].base);
            free(buffers[0].base);
            buffers.pop_front();
        }
        buff_idx = 0;
        buff_offset_commit = buff_offset;
    }

    void reset() {
        buff_idx = 0;
        buff_offset = buff_offset_commit;
        printf("reset to offset %d\n", buff_offset);
    }
};


static uv_loop_t *loop;
// TODO make this client-associated
class UvStream the_stream;
std::unordered_map<std::string, std::vector<uint8_t>> tcpStore_;

void on_close(uv_handle_t* handle) {
    free(handle);
}


void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    printf("allocating %p for %zu\n", handle, suggested_size);
    buf->base = (char*) malloc(suggested_size);
    buf->len = suggested_size;
}

bool checkKeys(const std::vector<std::string>& keys) const {
  return std::all_of(keys.begin(), keys.end(), [this](const std::string& s) {
    return tcpStore_.count(s) > 0;
  });
}



bool parse_set_command() {
    //1 byte command SET (done by the outer loop)
    //key: 1 string
    //data: 1 vector

    std::string key;
    if(!the_stream.read_str(key))
        return false;

    std::vector<uint8_t> data;
    if(!the_stream.read_vector(data))
        return false;

    the_stream.commit();
    printf("adding key %s with %zu bytes\n", key.c_str(), data.size());
    tcpStore_[key] = data;

    // On "set", wake up all clients that have been waiting
    //   wakeupWaitingClients(key);
    //   // Send key update to all watching clients
    //   newKey ? sendKeyUpdatesToClients(
    //                key, WatchResponseType::KEY_CREATED, oldData, newData)
    //          : sendKeyUpdatesToClients(
    //                key, WatchResponseType::KEY_UPDATED, oldData, newData);

    return true;
}


bool parse_wait_command(uv_stream_t *client) {
    //1 byte command SET (done by the outer loop)
    //key_count : int64_t
    //key_count x strings
    uint64_t key_count = 0;
    if(!the_stream.read_value(key_count))
        return false;

    std::vector<std::string> keys(key_count);
    for(auto i = 0; i < key_count; ++i) {
        if(!the_stream.read_str(keys[i]))
            return false;
    }

    the_stream.commit();

    printf("WAIT %zu keys\n", key_count);
    for(auto i = 0; i < key_count; ++i) {
        printf("\t[%d] %s\n", i, keys[i].c_str());
    }

  if (checkKeys(keys)) {
    tcputil::sendValue<WaitResponseType>(
        socket, WaitResponseType::STOP_WAITING);
  } else {
    printf("TODO implement wait\n");
  }
//     int numKeysToAwait = 0;
//     for (auto& key : keys) {
//       // Only count keys that have not already been set
//       if (tcpStore_.find(key) == tcpStore_.end()) {
//         waitingSockets_[key].push_back(socket);
//         numKeysToAwait++;
//       }
//     }
//     keysAwaited_[socket] = numKeysToAwait;
//   }

    return true;
}

void read_callback(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
    printf("read cb %zu\n", nread);
    if (nread < 0) {
        if (nread != UV_EOF)
            printf("Read error %s\n", uv_err_name(nread));
        else
            printf("disconnect?\n");
        uv_close((uv_handle_t*) client, on_close);
        return;
    }
    auto tmp = *buf;
    tmp.len = nread;
    the_stream.append(tmp);
    while(true) {
        the_stream.reset();
        uint8_t command = -1;
        if(!the_stream.read1(command))
            break;
        switch (command) {
        case SET:
            if(!parse_set_command())
                return;
            break;
        case WAIT:
            if(!parse_wait_command(client))
                return;
            break;
                
        default:
            printf("invalid command %d\n", command);
            uv_close((uv_handle_t*) client, on_close);
            return;
        }
    }
}


void on_new_connection(uv_stream_t *server, int status){
    printf("on_new_connection status %d\n", status);
    if (status < 0){ 
        printf("Accept error: %s\n", uv_strerror(status));
        return;
    }

    uv_tcp_t *client = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(loop, client);
    if (uv_accept(server, (uv_stream_t*) client) == 0) {
        printf("accept all good, starting to read\n");
        uv_read_start((uv_stream_t*) client, alloc_buffer, read_callback);
    } else {
        printf("failed to accept socket\n");
        uv_close((uv_handle_t*) client, on_close);

    }
}

int main() {
    struct sockaddr_in addr;
    printf("oi\n");
    loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
    uv_loop_init(loop);

    uv_tcp_t server;
    uv_tcp_init(loop, &server);
    uv_ip4_addr("0.0.0.0", PORT, &addr);

    uv_tcp_bind(&server, (const struct sockaddr*)&addr, 0);
    int r = uv_listen((uv_stream_t*)&server, DEFAULT_BACKLOG, on_new_connection);
    if(r) {
        printf("Listerning error: %s\n", uv_strerror(r));
        return 1;
    }
    printf("init done\n");
    int res = uv_run(loop, UV_RUN_DEFAULT);


    uv_loop_close(loop);
    free(loop);
    return 0;
}