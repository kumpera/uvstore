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

/*
TODO:
    Move the commit logic to the main loop instead of on each command, it sucks
    Move to use refcount UvClient
    Add MSG_NOSIGNAL support (is it possible?)
    cache alloc_buffer or, at least, make it return smaller sizes
*/

class UvClient;

void write_done(uv_write_t *req, int status) ;
void on_close(uv_handle_t* handle);
bool checkKeys(const std::vector<std::string>& keys);
void wakeupWaitingClients(const std::string& key);
void sendKeyUpdatesToClients(
    const std::string& key,
    WatchResponseType type,
    std::vector<uint8_t>& oldData,
    std::vector<uint8_t>& newData);

static uv_loop_t *loop;
// TODO make this client-associated
std::unordered_map<std::string, std::vector<uint8_t>> tcpStore_;
// From key -> the list of UvClient waiting on the key
std::unordered_map<std::string, std::vector<UvClient *>> waitingSockets_;
// From socket -> number of keys awaited
std::unordered_map<UvClient *, size_t> keysAwaited_;
//   // From key -> the list of sockets watching the key
  std::unordered_map<std::string, std::vector<UvClient *>> watchedSockets_;


class StreamWriter {
    std::vector<uint8_t> data;
    uv_write_t req;
    uv_buf_t buf;

public:
    StreamWriter() {} 

    void write1(uint8_t val) {
        data.push_back(val);
    }

    void write_vector(const std::vector<uint8_t> &val) {
        uint64_t size = val.size();
        uint8_t *size_ptr = (uint8_t*)&size;
        data.insert(data.end(), size_ptr, size_ptr + 8);
        data.insert(data.end(), val.begin(), val.end());
    }

    void write_string(const std::string &val) {
        uint64_t size = val.size();
        uint8_t *size_ptr = (uint8_t*)&size;
        data.insert(data.end(), size_ptr, size_ptr + 8);
        data.insert(data.end(), val.data(), val.data() + val.size());
    }

    void send(uv_stream_t *client){
        buf = uv_buf_init((char*)data.data(), data.size());
        uv_write(&req, client, &buf, 1, write_done);
    }

    static StreamWriter* from_write_request(uv_write_t *req) {
        size_t offset = offsetof(StreamWriter, req);
        return reinterpret_cast<StreamWriter*>(reinterpret_cast<char*>(req) - offset);
    }
};

class ChunkedStream {
    std::deque<uv_buf_t> buffers;
    int buff_idx;
    int buff_offset;
    int buff_idx_commit;
    int buff_offset_commit;

public:
    ChunkedStream(): buff_idx(0), buff_offset(0), buff_offset_commit(0) { }

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
        // printf("commiting %d [%d] bs:%zu bisz:%zu\n", buff_idx, buff_offset, buffers.size(), buffers[buff_idx].len);
        if(buff_idx >= buffers.size() || buff_offset >= buffers[buff_idx].len) {
            // printf("\treset offset, full use\n");
            buff_offset = 0;
            ++buff_idx;
        }

        for(int i = 0; i < buff_idx; ++i) {
            // printf("deleting [%d] %p\n", i, buffers[0].base);
            free(buffers[0].base);
            buffers.pop_front();
        }
        buff_idx = 0;
        buff_offset_commit = buff_offset;
    }

    void reset() {
        buff_idx = 0;
        buff_offset = buff_offset_commit;
        // printf("reset to offset %d\n", buff_offset);
    }
};

class UvClient {
    uv_tcp_t client;
    ChunkedStream stream;

public:

    UvClient(uv_loop_t *loop) {
            uv_tcp_init(loop, &client);
    }

    uv_stream_t* as_stream() {
        return (uv_stream_t*)&client;
    }

    static UvClient* from_handle(uv_handle_t *handle) {
        size_t offset = offsetof(UvClient, client);
        return reinterpret_cast<UvClient*>(reinterpret_cast<char*>(handle) - offset);
    }

    void process_buf(const uv_buf_t *buf, size_t nread) {
        auto tmp = *buf;
        tmp.len = nread;
        stream.append(tmp);
        while(true) {
            stream.reset();
            uint8_t command = -1;
            if(!stream.read1(command))
                break;
            switch ((QueryType)command) {
            case QueryType::SET:
                if(!parse_set_command())
                    return;
                break;
            case QueryType::GET:
                if(!parse_get_command())
                    return;
                break;
            case QueryType::ADD:
                if(!parse_add_command())
                    return;
                break;
            case QueryType::WAIT:
                if(!parse_wait_command())
                    return;
                break;
                    
            default:
                printf("invalid command %d\n", command);
                uv_close((uv_handle_t*) &client, on_close);
                return;
            }
        }
    }

    bool parse_set_command() {
        //1 byte command SET (done by the outer loop)
        //key: 1 string
        //data: 1 vector

        std::string key;
        if(!stream.read_str(key))
            return false;

        std::vector<uint8_t> newData;
        if(!stream.read_vector(newData))
            return false;

        stream.commit();
        printf("adding key %s with %zu bytes\n", key.c_str(), newData.size());

        std::vector<uint8_t> oldData;
        bool newKey = true;
        auto it = tcpStore_.find(key);
        if (it != tcpStore_.end()) {
            oldData = it->second;
            newKey = false;
        }
        tcpStore_[key] = newData;

        // On "set", wake up all clients that have been waiting
        wakeupWaitingClients(key);
        // Send key update to all watching clients
        newKey ? sendKeyUpdatesToClients(
                    key, WatchResponseType::KEY_CREATED, oldData, newData)
                : sendKeyUpdatesToClients(
                    key, WatchResponseType::KEY_UPDATED, oldData, newData);

        return true;
    }

    bool parse_wait_command() {
        //1 byte command SET (done by the outer loop)
        //key_count : int64_t
        //key_count x strings
        uint64_t key_count = 0;
        if(!stream.read_value(key_count))
            return false;

        std::vector<std::string> keys(key_count);
        for(auto i = 0; i < key_count; ++i) {
            if(!stream.read_str(keys[i]))
                return false;
        }

        stream.commit();

        printf("WAIT %zu keys\n", key_count);
        for(auto i = 0; i < key_count; ++i) {
            printf("\t[%d] %s\n", i, keys[i].c_str());
        }

        if (checkKeys(keys)) {
            StreamWriter* sw = new StreamWriter();
            sw->write1((uint8_t)WaitResponseType::STOP_WAITING);
            sw->send(as_stream());
        } else {
            printf("TODO implement wait\n");
            int numKeysToAwait = 0;
            for (auto& key : keys) {
                // Only count keys that have not already been set
                if (tcpStore_.find(key) == tcpStore_.end()) {
                    waitingSockets_[key].push_back(this);
                    numKeysToAwait++;
                }
            }
            keysAwaited_[this] = numKeysToAwait;
        }

        return true;
    }

    bool parse_get_command() {
        //1 byte command SET (done by the outer loop)
        //key: 1 string


        std::string key;
        if(!stream.read_str(key))
            return false;
        stream.commit();

        auto data = tcpStore_.at(key);
        StreamWriter* sw = new StreamWriter();
        sw->write_vector(data);
        sw->send(as_stream());

        return true;
    }

    bool parse_add_command() {
        //1 byte command SET (done by the outer loop)
        //key: 1 string


        std::string key;
        if(!stream.read_str(key))
            return false;
        stream.commit();

        auto data = tcpStore_.at(key);
        StreamWriter* sw = new StreamWriter();
        sw->write_vector(data);
        sw->send(as_stream());

        return true;
    }


};


void write_done(uv_write_t *req, int status) {
    printf("write done for %p\n", req);
    if (status) {
        printf("Write error %s\n", uv_strerror(status));
    }

    StreamWriter *sw = StreamWriter::from_write_request(req);
    delete sw;
}

void on_close(uv_handle_t* handle) {
    UvClient *client = UvClient::from_handle(handle);
    delete client;
}


void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    // printf("allocating %p for %zu\n", handle, suggested_size);
    buf->base = (char*) malloc(suggested_size);
    buf->len = suggested_size;
}

bool checkKeys(const std::vector<std::string>& keys) {
  return std::all_of(keys.begin(), keys.end(), [](const std::string& s) {
    return tcpStore_.count(s) > 0;
  });
}


void wakeupWaitingClients(const std::string& key) {
  auto socketsToWait = waitingSockets_.find(key);
  if (socketsToWait != waitingSockets_.end()) {
    for (UvClient *client : socketsToWait->second) {
      if (--keysAwaited_[client] == 0) {
        printf("waking up client due to key %s\n", key.c_str());
        StreamWriter* sw = new StreamWriter();
        sw->write1((uint8_t)WaitResponseType::STOP_WAITING);
        sw->send(client->as_stream());
      }
    }
    waitingSockets_.erase(socketsToWait);
  }
}


void sendKeyUpdatesToClients(
    const std::string& key,
    WatchResponseType type,
    std::vector<uint8_t>& oldData,
    std::vector<uint8_t>& newData) {
  for (UvClient *client : watchedSockets_[key]) {
        StreamWriter* sw = new StreamWriter();
        sw->write1((uint8_t)type);
        sw->write_string(key);
        sw->write_vector(oldData);
        sw->write_vector(newData);


        sw->send(client->as_stream());

    // tcputil::sendValue<WatchResponseType>(socket, type);
    // tcputil::sendString(socket, key, true);
    // tcputil::sendVector<uint8_t>(socket, oldData);
    // tcputil::sendVector<uint8_t>(socket, newData);
  }
}


void read_callback(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
    // printf("read cb %zu\n", nread);
    if (nread < 0) {
        if (nread != UV_EOF)
            printf("Read error %s\n", uv_err_name(nread));
        // else
        //     printf("disconnect?\n");
        uv_close((uv_handle_t*) client, on_close);
        return;
    }
    UvClient *uv_client = UvClient::from_handle((uv_handle_t*)client);
    uv_client->process_buf(buf, nread);
}


void on_new_connection(uv_stream_t *server, int status){
    // printf("on_new_connection status %d\n", status);
    if (status < 0){ 
        printf("Accept error: %s\n", uv_strerror(status));
        return;
    }

    UvClient *client = new UvClient(loop);
    if (uv_accept(server, client->as_stream()) == 0) {
        // printf("accept all good, starting to read\n");
        uv_read_start((uv_stream_t*) client->as_stream(), alloc_buffer, read_callback);
    } else {
        printf("failed to accept socket\n");
        uv_close((uv_handle_t*) client->as_stream(), on_close);

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