// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <braft/configuration.h>
#include <braft/protobuf_file.h>  // braft::ProtoBufFile
#include <braft/raft.h>           // braft::Node braft::StateMachine
#include <braft/storage.h>        // braft::SnapshotWriter
#include <braft/util.h>           // braft::AsyncClosureGuard
#include <brpc/controller.h>      // brpc::Controller
#include <brpc/server.h>          // brpc::Server
#include <butil/iobuf.h>
#include <gflags/gflags.h>  // DEFINE_*

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "config_manager.h"
#include "mbraft.pb.h"  // AppendEntriesRPC
#include "utils.h"

namespace mbraft {

#define INVALIED_GROUP_IDX -1
#define INVALID_LEADER_COUNT -1

class MulitGroupRaftManager;

// Log view by upper layer.
// 1. Can serialize/deserialize the log to/from butil::IOBuf
// [int32 type] [uint32 length] [log data]
class UpperLog {
   public:
    enum LogType {
        INVALID = 0,
        SPLIT = 1,
        MERGE_SOURCE = 2,  // source group should stop
        MERGE_TARGET = 3,
        NORMAL = 4,
    };

   public:
    // Default constructor
    UpperLog() : type(INVALID), length(0) {}

    // Constructor with parameters
    UpperLog(LogType type, const std::string &data)
        : type(type), length(data.size()), data(data) {}

    // Serialize the log to butil::IOBuf
    void serialize_to(butil::IOBuf *buf) const {
        buf->append(&type, sizeof(type));
        uint32_t len = static_cast<uint32_t>(length);
        buf->append(&len, sizeof(len));
        buf->append(data);
    }

    // Deserialize the log from butil::IOBuf
    void deserialize_from(butil::IOBuf *buf) {
        CHECK(buf->size() >= sizeof(type) + sizeof(length));
        buf->cutn(&type, sizeof(type));
        uint32_t len;
        buf->cutn(&len, sizeof(len));
        CHECK(buf->size() >= len);
        length = len;
        buf->cutn(&data, length);
    }

    // Getters
    int32_t get_type() const { return type; }
    uint32_t get_length() const { return length; }
    const std::string &get_data() const { return data; }

    // Setters
    void set_type(LogType type) { this->type = type; }
    void set_data(const std::string &data) {
        this->data = data;
        this->length = data.size();
    }

   private:
    LogType type;
    uint32_t length;
    std::string data;
};

struct SingleMachineOptions {
    MulitGroupRaftManager *raft_manager = nullptr;
    braft::Configuration peers;
    braft::PeerId peer_id;
    braft::GroupId group_id;
    int32_t group_idx = INVALIED_GROUP_IDX;
    std::string data_path = ".";
};

// Implement the simplest state machine as a raft group.
class SingleMachine : public braft::StateMachine {
   public:
    int append_task(const UpperLog &upper_log, braft::Closure *done = nullptr) {
        if (!_node) {
            LOG(ERROR) << "Node is not initialized";
            return -1;
        }

        if (!_is_leader) {
            LOG(ERROR) << "This node is not leader";
            return -1;
        }

        butil::IOBuf log;
        upper_log.serialize_to(&log);

        braft::Task task;
        task.data = &log;
        task.done = done;

        _node->apply(task);
        return 0;
    }

    int append_split_log(NewConfiguration &new_conf,
                         braft::Closure *done = nullptr);
    int append_merge_log(bool is_source = false,
                         braft::Closure *done = nullptr);

   public:
    int init(SingleMachineOptions &options);
    bool is_leader() { return _is_leader; }
    int change_leader_to(braft::PeerId to);
    int request_leadership();

    void on_apply(braft::Iterator &iter) override;
    void on_snapshot_save(braft::SnapshotWriter *writer,
                          braft::Closure *done) override;
    int on_snapshot_load(braft::SnapshotReader *reader) override;
    void on_leader_start(int64_t term) override;
    void on_leader_stop(const butil::Status &status) override;
    void on_shutdown() override;
    void on_error(const ::braft::Error &e) override;
    void on_configuration_committed(
        const ::braft::Configuration &conf) override;
    void on_stop_following(const ::braft::LeaderChangeContext &ctx) override;
    void on_start_following(const ::braft::LeaderChangeContext &ctx) override;

   private:
    int start_servic();

   private:
    std::unique_ptr<braft::Node> _node;
    MulitGroupRaftManager *_raft_manager = nullptr;
    int32_t _group_idx = INVALIED_GROUP_IDX;
    braft::PeerId _peer_id;
    bool _is_leader = false;
    std::unique_ptr<brpc::Server> _server;
};

}  // namespace mbraft