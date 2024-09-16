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

#include "config_manager.h"
#include "mbraft.pb.h" // AppendEntriesRPC
#include "single_state_machine.h"
#include <atomic>
#include <braft/configuration.h>
#include <braft/protobuf_file.h> // braft::ProtoBufFile
#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <gflags/gflags.h> // DEFINE_*
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
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
};

// Manage all the raft groups, limit all the leader on one node.
class MulitGroupRaftManager {
  friend class SingleMachine;

  enum GroupState {
    ELECTION, // This group is in election, will do leader change after
              // election done.
    FOLLOWER, // This group's election is done before coordinating, need to
              // trigger leader change.
    LEADER_CHANGING, // already send leader change request.
    LEADER           // No need to change leader.
  };

  enum ManagerState {
    COORDINATING, // Coordinating leader change.
    LEADING,      // This node is leader node.
    NORMAL        // Normal state.
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

private:
  void on_leader_start(int32_t group_id);
  int on_start_following(int32_t group_id);
  static void *send_change_leader_req(void *machine);

private:
  std::vector<StateMachine> _machines;
  ConfigurationManager _config_manager;

  // Coordination
private:
  int _start_leader_change(int32_t group_id);
  bool _is_coordinating() { return _state == COORDINATING; }

  ManagerState _state = NORMAL;
  std::mutex _cood_mutex;
  std::atomic<size_t> _needed_count{0}; // The number of groups need to be
                                        // coordinated.
};

class MbraftServiceImpl : public MbraftService {
public:
  void leader_change(::google::protobuf::RpcController *controller,
                     const LeaderChangeRequest *request,
                     LeaderChangeResponse *response,
                     ::google::protobuf::Closure *done) override {
    brpc::ClosureGuard done_guard(done);
    assert(_machine != nullptr);
    int res{0};

    res = _machine->change_leader_to(braft::PeerId(request->change_to()));
    response->set_success(res == 0);
    if (res != 0) {
      LOG(ERROR) << "Fail to change leader to " << request->change_to();
    }
  }

private:
  SingleMachine *_machine = nullptr;
};

} // namespace mbraft
