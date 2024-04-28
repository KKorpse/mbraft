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

#include <atomic>
#include <braft/configuration.h>
#include <braft/protobuf_file.h> // braft::ProtoBufFile
#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <cstddef>
#include <cstdint>
#include <gflags/gflags.h> // DEFINE_*
#include <vector>

namespace mbraft {

#define INVALIED_GROUP_ID -1

class MulitGroupRaftManager;
struct SingleMachineOptions {
  MulitGroupRaftManager *raft_manager = nullptr;
};

// Implement the simplest state machine as a raft group.
class SingleMachine : public braft::StateMachine {
public:
  void init(SingleMachineOptions &options) {
    CHECK(options.raft_manager != nullptr);
    _raft_manager = options.raft_manager;
  }

  void on_apply(braft::Iterator &iter);

  void on_snapshot_save(braft::SnapshotWriter *writer, braft::Closure *done);

  int on_snapshot_load(braft::SnapshotReader *reader);

  void on_leader_start(int64_t term);
  void on_leader_stop(const butil::Status &status);

  void on_shutdown();
  void on_error(const ::braft::Error &e);
  void on_configuration_committed(const ::braft::Configuration &conf);
  void on_stop_following(const ::braft::LeaderChangeContext &ctx);
  void on_start_following(const ::braft::LeaderChangeContext &ctx);

private:
  braft::Node *volatile _node;
  MulitGroupRaftManager *_raft_manager = nullptr;
  int32_t _group_id = INVALIED_GROUP_ID;
};

struct MulitGroupRaftManagerOptions {
  // The number of groups.
  int32_t group_count = INVALIED_GROUP_ID;
};

// Manage all the raft groups, limit all the leader on one node.
class MulitGroupRaftManager {
public:
  MulitGroupRaftManager();
  ~MulitGroupRaftManager() {}

  void init(MulitGroupRaftManagerOptions &options) {
    CHECK_GE(options.group_count, 0);
    for (int32_t i = 0; i < options.group_count; ++i) {
      SingleMachineOptions machine_options;
      machine_options.raft_manager = this;
      _machines.emplace_back();
      _machines.back().init(machine_options);
    }

    
  }

private:
  // end of @braft::StateMachine

private:
  std::vector<SingleMachine> _machines;

  // The number of leaders on this node.
  // When the _machines[0] is the leader of the group, set _leader_count to 1
  // and change all the other group's leader to this node.
  std::atomic<int32_t> _leader_count{0};
};

} // namespace mbraft