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

#include "mbraft.pb.h" // AppendEntriesRPC
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
#include <vector>
namespace mbraft {

#define INVALIED_GROUP_ID -1
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

class MulitGroupRaftManager;
struct SingleMachineOptions {
  MulitGroupRaftManager *raft_manager = nullptr;
};

// Implement the simplest state machine as a raft group.
class SingleMachine : public braft::StateMachine {
public:
  void init(SingleMachineOptions &options);
  bool is_leader() { return _is_leader; }
  void change_leader_to(braft::PeerId to);
  void request_leadership();

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
  bool _is_leader = false;
};

struct MulitGroupRaftManagerOptions {
  // The number of groups.
  int32_t group_count = INVALIED_GROUP_ID;
};

// Manage all the raft groups, limit all the leader on one node.
class MulitGroupRaftManager {
  friend class SingleMachine;

public:
  MulitGroupRaftManager(){};
  ~MulitGroupRaftManager() {}

  void init(MulitGroupRaftManagerOptions &options) {
    CHECK_GE(options.group_count, 0);
    for (int32_t i = 0; i < options.group_count; ++i) {
      SingleMachineOptions machine_options;
      machine_options.raft_manager = this;
      _machines.emplace_back();
      _machines.back()->init(machine_options);
    }
  }

private:
  void on_leader_start(int32_t group_id);
  void on_start_following(int32_t group_id);
  void coordinate_leader_if_need();

private:
  std::vector<std::unique_ptr<SingleMachine>> _machines;

  // When the _machines[0] becomes the leader of the group, set it to true
  // Change all the other group's leader to this node.
  bool _wait_for_coordinate = false;
  std::unique_ptr<SynchronizedCountClosure> _coordinate_clousure{nullptr};
  std::atomic<size_t> _election_done_count{0};
};

class MbraftServiceImpl : public MbraftService {
public:
  void leader_change(::google::protobuf::RpcController *controller,
                     const LeaderChangeRequest *request,
                     LeaderChangeResponse *response,
                     ::google::protobuf::Closure *done) override {
    brpc::ClosureGuard done_guard(done);
    assert(_machine != nullptr);
    _machine->change_leader_to(braft::PeerId(request->change_to()));
  }

private:
  SingleMachine *_machine = nullptr;
};

} // namespace mbraft