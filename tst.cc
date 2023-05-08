#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <deque>
#include <unordered_map>
#include <algorithm>
#include <sstream>

#define PORT 29501
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
  APPEND,
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
    wrap a simplified wrapper around StreamWriter
    expose watchKey to python (I got a commit to expose check)
    proper error handling
    logging
    telemetry
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

    template <typename T>
    void write_value(T val) {
        uint8_t *val_ptr = (uint8_t*)&val;
        data.insert(data.end(), val_ptr, val_ptr + sizeof(T));
    }

    void write_vector(const std::vector<uint8_t> &val) {
        write_value<uint64_t>(val.size());
        data.insert(data.end(), val.begin(), val.end());
    }

    void write_string(const std::string &val) {
        write_value<uint64_t>(val.size());
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
    int capacity;
    int buff_idx_commit;
    int buff_offset_commit;
    int read_offset;

public:
    ChunkedStream(): buff_idx(0), buff_offset(0), capacity(0), buff_idx_commit(0), buff_offset_commit(0), read_offset(0) { }

    size_t buf_count() { return buffers.size(); }

    void append(uv_buf_t buf) {
        if(buf.len == 0) {
            free(buf.base);
        } else {
            capacity += buf.len;
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
        ++read_offset;
        return true;
    }

    template <typename T>
    bool read_value(T& value) {
        uint8_t *val = (uint8_t*)&value;
        if(available() < sizeof(T))
            return false;

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
        if(available() < size)
            return false;
        str.reserve(size);
        //TODO optimize this with larger chunks copies
        for(int i = 0; i < size; ++i) {
            char c;
            if(!read_value(c))
                return false;
            str.push_back(c);
        }
        return true;
    }

    template <typename T>
    bool read_vector(std::vector<T> &data) {
        uint64_t size = 0;
        if(!read_value(size))
            return false;
        if(available() < size)
            return false;
        data.reserve(size);
        //TODO optimize this with larger chunks copies
        for(int i = 0; i < size; ++i) {
            T tmp = {};
            if(!read_value(tmp))
                return false;
            data.push_back(tmp);
        }
        return true;
    }

    int available() {
        return capacity - read_offset;
    }

    void commit () {
        if(buff_idx >= buffers.size() || buff_offset >= buffers[buff_idx].len) {
            buff_offset = 0;
            ++buff_idx;
        }

        for(int i = 0; i < buff_idx; ++i) {
            free(buffers[0].base);
            capacity -= buffers[0].len;
            buffers.pop_front();
        }
        buff_idx = 0;
        read_offset = buff_offset_commit = buff_offset;
    }

    void reset() {
        buff_idx = 0;
        read_offset = buff_offset = buff_offset_commit;

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
            case QueryType::COMPARE_SET:
                if(!parse_compare_set_command())
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
            case QueryType::CHECK:
                if(!parse_check_command())
                    return;
                break;
            case QueryType::WAIT:
                if(!parse_wait_command())
                    return;
                break;
            case QueryType::GETNUMKEYS:
                if(!parse_getnumkeys_command())
                    return;
                break;
            case QueryType::WATCH_KEY:
                if(!parse_watch_key_command())
                    return;
                break;
            case QueryType::DELETE_KEY:
                if(!parse_delete_key_command())
                    return;
                break;
            default:
                printf("invalid command %d\n", command);
                uv_close((uv_handle_t*) &client, on_close);
                return;
            }
            stream.commit();

        }
    }

    bool parse_set_command() {
        //key: string
        //data: vector

        std::string key;
        if(!stream.read_str(key))
            return false;

        std::vector<uint8_t> newData;
        if(!stream.read_vector(newData))
            return false;

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
        //key_count: int64_t
        //keys: string[key_count]

        size_t key_count = 0;
        if(!stream.read_value(key_count))
            return false;

        if (key_count > 10000) {
            printf("\twait %p size %zu\n", this, key_count);
            throw std::runtime_error("size too big");
        }
        std::vector<std::string> keys(key_count);
        for(auto i = 0; i < key_count; ++i) {
            if(!stream.read_str(keys[i]))
                return false;
        }

        if (checkKeys(keys)) {
            StreamWriter* sw = new StreamWriter();
            sw->write1((uint8_t)WaitResponseType::STOP_WAITING);
            sw->send(as_stream());
        } else {
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
        //key: string

        std::string key;
        if(!stream.read_str(key))
            return false;

        auto data = tcpStore_.at(key);
        StreamWriter* sw = new StreamWriter();
        sw->write_vector(data);
        sw->send(as_stream());

        return true;
    }

    bool parse_add_command() {
        //key: string
        //addVal: int64

        std::string key;
        if(!stream.read_str(key))
            return false;

        int64_t addVal = 0;
        if(!stream.read_value(addVal))
            return false;

        bool newKey = true;
        std::vector<uint8_t> oldData;
        auto it = tcpStore_.find(key);
        if (it != tcpStore_.end()) {
            oldData = it->second;
            auto buf = reinterpret_cast<const char*>(it->second.data());
            auto len = it->second.size();
            addVal += std::stoll(std::string(buf, len));
            newKey = false;
        }
        auto addValStr = std::to_string(addVal);
        std::vector<uint8_t> newData = std::vector<uint8_t>(addValStr.begin(), addValStr.end());
        tcpStore_[key] = newData;
        // Now send the new value

        StreamWriter* sw = new StreamWriter();
        sw->write_value(addVal);
        sw->send(as_stream());

        // On "add", wake up all clients that have been waiting
        wakeupWaitingClients(key);
        // Send key update to all watching clients
        newKey ? sendKeyUpdatesToClients(
                key, WatchResponseType::KEY_CREATED, oldData, newData)
            : sendKeyUpdatesToClients(
                key, WatchResponseType::KEY_UPDATED, oldData, newData);

        return true;
    }

    bool parse_compare_set_command() {
        //key: string
        //current: vector
        //new: vector
        std::string key;
        if(!stream.read_str(key))
            return false;

        std::vector<uint8_t> currentValue;
        if(!stream.read_vector(currentValue))
            return false;

        std::vector<uint8_t> newValue;
        if(!stream.read_vector(newValue))
            return false;

        auto pos = tcpStore_.find(key);
        if (pos == tcpStore_.end()) {
            if (currentValue.empty()) {
                tcpStore_[key] = newValue;

                // Send key update to all watching clients
                sendKeyUpdatesToClients(
                    key, WatchResponseType::KEY_CREATED, currentValue, newValue);

                StreamWriter* sw = new StreamWriter();
                sw->write_vector(newValue);
                sw->send(as_stream());
            } else {
                // TODO: This code path is not ideal as we are "lying" to the caller in
                // case the key does not exist. We should come up with a working solution.
                StreamWriter* sw = new StreamWriter();
                sw->write_vector(currentValue);
                sw->send(as_stream());
            }
        } else {
            if (pos->second == currentValue) {
                pos->second = std::move(newValue);

                // Send key update to all watching clients
                sendKeyUpdatesToClients(
                    key, WatchResponseType::KEY_UPDATED, currentValue, pos->second);
            }

            StreamWriter* sw = new StreamWriter();
            sw->write_vector(pos->second);
            sw->send(as_stream());
        }
        return true;
    }

    bool parse_check_command() {
        //key_count : int64_t
        //keys: string[key_count]

        uint64_t key_count = 0;
        if(!stream.read_value(key_count))
            return false;

        std::vector<std::string> keys(key_count);
        for(auto i = 0; i < key_count; ++i) {
            if(!stream.read_str(keys[i]))
                return false;
        }

        // Now we have received all the keys
        StreamWriter* sw = new StreamWriter();
        if (checkKeys(keys)) {
            sw->write_value(CheckResponseType::READY);
        } else {
            sw->write_value(CheckResponseType::NOT_READY);
        }
        sw->send(as_stream());
        return true;
    }

    bool parse_getnumkeys_command() {
        StreamWriter* sw = new StreamWriter();
        sw->write_value<int64_t>(tcpStore_.size());
        sw->send(as_stream());

        return true;
    }

    bool parse_delete_key_command() {
        //key: string

        std::string key;
        if(!stream.read_str(key))
            return false;

        auto it = tcpStore_.find(key);
        if (it != tcpStore_.end()) {
        std::vector<uint8_t> oldData = it->second;
        // Send key update to all watching clients
        std::vector<uint8_t> newData;
        sendKeyUpdatesToClients(
        key, WatchResponseType::KEY_DELETED, oldData, newData);
        }
        auto numDeleted = tcpStore_.erase(key);

        StreamWriter* sw = new StreamWriter();
        sw->write_value<int64_t>(numDeleted);
        sw->send(as_stream());

        return true;
    }

    bool parse_watch_key_command() {
        //key: string

        std::string key;
        if(!stream.read_str(key))
            return false;

        // Record the socket to respond to when the key is updated
        watchedSockets_[key].push_back(this);

        // Send update to TCPStoreWorkerDaemon on client

        StreamWriter* sw = new StreamWriter();
        sw->write_value(WatchResponseType::KEY_CALLBACK_REGISTERED);
        sw->send(as_stream());
        return true;
    }
};


void write_done(uv_write_t *req, int status) {
    if (status) {
        printf("Write error %s\n", uv_strerror(status));
        //TODO close the stream?
    }

    // printf("freeing write request %p\n", req);
    StreamWriter *sw = StreamWriter::from_write_request(req);
    delete sw;
}

void on_close(uv_handle_t* handle) {
    UvClient *client = UvClient::from_handle(handle);

    delete client;
}


void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    //TODO caching
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
  }
}


void read_callback(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
    if (nread < 0) {
        if (nread != UV_EOF)
            printf("Read error %s\n", uv_err_name(nread));
        uv_close((uv_handle_t*) client, on_close);
        return;
    }
    if(nread > 0) {
        try {
            UvClient *uv_client = UvClient::from_handle((uv_handle_t*)client);
            uv_client->process_buf(buf, nread);
        } catch(std::exception &ex) {
            uv_close((uv_handle_t*) client, on_close);;
        }
    }
}


void on_new_connection(uv_stream_t *server, int status){
    if (status < 0){ 
        printf("Accept error: %s\n", uv_strerror(status));
        return;
    }

    UvClient *client = new UvClient(loop);
    if (uv_accept(server, client->as_stream()) == 0) {
        uv_read_start((uv_stream_t*) client->as_stream(), alloc_buffer, read_callback);
    } else {
        printf("failed to accept socket\n");
        uv_close((uv_handle_t*) client->as_stream(), on_close);
    }
}

int main() {
    struct sockaddr_in addr;
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