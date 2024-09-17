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

class SynchronizedCountClosure : public braft::Closure {
   public:
    explicit SynchronizedCountClosure(int num_signal) : _count(num_signal) {}

    void Run() override {
        std::lock_guard<std::mutex> lock(_mutex);
        if (--_count <= 0) {
            _cv.notify_all();
        }
    }

    void wait() {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [this] { return _count <= 0; });
    }

    void reset(int num_signal) {
        std::lock_guard<std::mutex> lock(_mutex);
        _count = num_signal;
    }

   private:
    int _count;
    std::mutex _mutex;
    std::condition_variable _cv;
};

struct MulitGroupRaftManagerOptions {
    // The number of groups.
    int32_t group_count = INVALIED_GROUP_IDX;

    // Addr of each group on this node.
    std::vector<braft::PeerId> server_ids;

    // Peers of each group.
    std::vector<braft::Configuration> configs;

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
            state = FOLLOWER;
        }
        std::unique_ptr<SingleMachine> machine;
        GroupState state;
    };

   public:
    MulitGroupRaftManager(){};
    ~MulitGroupRaftManager() {}

    // Init all raft groups and start brpc service.
    void init_and_start(MulitGroupRaftManagerOptions &options);
    ManagerState state() { return _state; }

    // group_idx: the index of the group in the _machines.
    // conf: the new configuration of the group.
    int split_raft_group(int32_t group_idx, braft::Configuration &conf);

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

    ManagerState _state = NORMAL;
    std::mutex _cood_mutex;
    std::atomic<size_t> _needed_count{0};  // The number of groups need to be
                                           // coordinated.
};

class SingleMachineServiceImpl : public SingleMachineService {
   public:
    explicit SingleMachineServiceImpl(SingleMachine *machine) : _machine(machine) {}
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
