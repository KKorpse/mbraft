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
#include <gflags/gflags.h>        // DEFINE_*

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

namespace mbraft {

#define INVALIED_GROUP_IDX -1
#define INVALID_LEADER_COUNT -1

class MulitGroupRaftManager;
struct SingleMachineOptions {
    MulitGroupRaftManager *raft_manager = nullptr;
    braft::Configuration peers;
    braft::PeerId addr;
    braft::GroupId group_id;
    int32_t group_idx = INVALIED_GROUP_IDX;
    std::string data_path = ".";
};

// Implement the simplest state machine as a raft group.
class SingleMachine : public braft::StateMachine {
   public:
    void init(SingleMachineOptions &options);
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
    int32_t _group_id = INVALIED_GROUP_IDX;
    bool _is_leader = false;
};

}  // namespace mbraft