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
#include <unordered_map>
#include <vector>

#include "config_manager.h"
#include "mbraft.pb.h"  // AppendEntriesRPC
#include "single_state_machine.h"
namespace mbraft {

#define INVALIED_GROUP_IDX -1
#define INVALID_LEADER_COUNT -1

struct MulitGroupRaftManagerOptions {
    // The number of groups.
    int32_t group_count = INVALIED_GROUP_IDX;

    std::string confg_file_path;

    std::string name;
};

// Manage all the raft groups, limit all the leader on one node.
class MulitGroupRaftManager {
    friend class SingleMachine;

    enum GroupState {
        ELECTION,  // This group is in election, will do leader change after
                   // election done.
        FOLLOWER,  // This group's election is done before coordinating, need to
                   // trigger leader change.
        LEADER_CHANGING,  // already send leader change request.
        LEADER            // No need to change leader.
    };

    enum ManagerState {
        COORDINATING,  // Coordinating leader change.
        LEADING,       // This node is leader node.
        NORMAL         // Normal state.
    };

    struct StateMachine {
        StateMachine(SingleMachine *mch) {
            CHECK(machine != nullptr);
            machine.reset(mch);
            state = ELECTION;
        }
        std::unique_ptr<SingleMachine> machine;
        GroupState state;
    };

   public:
    MulitGroupRaftManager(){};
    ~MulitGroupRaftManager() {}

    // Init all raft groups and start brpc service.
    int init_and_start(MulitGroupRaftManagerOptions &options);
    ManagerState state() { return _state; }

    // group_idx: the index of the group in the _machines.
    // conf: the new configuration of the group.
    // Only the leader of the group can call this function.
    int split_raft_group(int32_t group_idx, braft::Configuration &conf,
                         NewConfiguration &new_conf);

    // Merge source group to target group
    int merge_raft_group(int32_t source_group_id, int32_t target_group_id);

    // Add a new raft group to the manager.
    int add_raft_group(braft::Configuration &conf, braft::PeerId peer_id);

    int on_merge_source_apply();
    int on_merge_target_apply();

   private:
    void on_leader_start(int32_t group_idx);
    int on_start_following(int32_t group_idx);
    int on_leader_stop(int32_t group_idx);
    static void *send_change_leader_req(void *machine);

   private:
    std::vector<StateMachine> _machines;
    ConfigurationManager _config_manager;
    std::string _name;

    // Coordination
   private:
    int _start_leader_change(int32_t group_idx);
    bool _is_coordinating() { return _state == COORDINATING; }
    bool _is_leading() { return _state == LEADING; }
    bool _is_all_leader_on_this_node();

    ManagerState _state = NORMAL;
    std::mutex _mutex;
    // The number of groups need to be coordinated.
    std::atomic<size_t> _needed_count{0};

    // merge/split
   private:
    int32_t _target_group_id = INVALIED_GROUP_IDX;
};

class SingleMachineServiceImpl : public SingleMachineService {
   public:
    explicit SingleMachineServiceImpl(SingleMachine *machine)
        : _machine(machine) {}
    void leader_change(::google::protobuf::RpcController *controller,
                       const LeaderChangeRequest *request,
                       LeaderChangeResponse *response,
                       ::google::protobuf::Closure *done) override {
        brpc::ClosureGuard done_guard(done);
        CHECK(_machine != nullptr);

        LOG(INFO) << "Receive leader change request to "
                  << request->change_to();
        int res{0};
        res = _machine->change_leader_to(braft::PeerId(request->change_to()));
        if (res != 0) {
            LOG(ERROR) << "Fail to change leader to " << request->change_to()
                       << " res: " << res;
        }
        response->set_res(res);
    }

   private:
    SingleMachine *_machine = nullptr;
};

}  // namespace mbraft
