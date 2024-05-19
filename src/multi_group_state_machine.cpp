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

#include <braft/protobuf_file.h> // braft::ProtoBufFile
#include <braft/raft.h>          // braft::Node braft::StateMachine
#include <braft/storage.h>       // braft::SnapshotWriter
#include <braft/util.h>          // braft::AsyncClosureGuard
#include <brpc/channel.h>
#include <brpc/controller.h> // brpc::Controller
#include <brpc/server.h>     // brpc::Server
#include <bthread/bthread.h>
#include <cassert>
#include <gflags/gflags.h> // DEFINE_*
#include <memory>
#include <vector>

#include "mbraft.pb.h"
#include "multi_group_state_machine.h"

DEFINE_int32(
    election_timeout_ms, 5000,
    "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(snapshot_interval, 7200, "Interval between each snapshot");

namespace mbraft {

void SingleMachine::init(SingleMachineOptions &options) {
  CHECK(options.raft_manager != nullptr);
  _raft_manager = options.raft_manager;

  braft::NodeOptions node_options;
  node_options.initial_conf = options.peers;
  node_options.election_timeout_ms = FLAGS_election_timeout_ms;
  node_options.fsm = this;
  node_options.node_owns_fsm = false;
  node_options.snapshot_interval_s = FLAGS_snapshot_interval;
  std::string prefix = "local://" + options.data_path;
  node_options.log_uri = prefix + "/log";
  node_options.raft_meta_uri = prefix + "/raft_meta";
  node_options.snapshot_uri = prefix + "/snapshot";
  _node.reset(new braft::Node(options.group_id, options.addr));
  if (_node->init(node_options) != 0) {
    LOG(ERROR) << "Fail to init raft node";
    _node.reset(nullptr);
  }
}

void SingleMachine::change_leader_to(braft::PeerId to) {
  if (_node->transfer_leadership_to(to) != 0) {
    LOG(ERROR) << "Fail to transfer leader to " << to.to_string();
  }
}

void SingleMachine::request_leadership() {
  CHECK(!_is_leader);
  CHECK(_node != nullptr);
  auto leader = _node->leader_id();
  CHECK(!leader.is_empty());

  std::unique_ptr<brpc::Controller> cntl(new brpc::Controller());
  std::unique_ptr<LeaderChangeRequest> request(new LeaderChangeRequest);
  std::unique_ptr<LeaderChangeResponse> response(new LeaderChangeResponse);
  request->set_change_to(_node->node_id().peer_id.to_string());

  brpc::Channel channel;
  brpc::ChannelOptions channel_opt;
  channel_opt.connect_timeout_ms = 1000;
  channel_opt.timeout_ms = -1; // We don't need RPC timeout
  if (channel.Init(leader.addr, &channel_opt) != 0) {
    LOG(ERROR) << "Fail to init sending channel: "
               << _node->node_id().peer_id.to_string() << " to "
               << leader.to_string();
    return;
  }

  MbraftService_Stub stub(&channel);
  LOG(WARNING) << _node->node_id().peer_id.to_string()
               << " Request leadership from " << leader.to_string();
  stub.leader_change(cntl.get(), request.get(), response.get(), nullptr);
}

void SingleMachine::on_leader_start(int64_t term) {
  LOG(INFO) << "Node " << _node->node_id() << " starts to be leader";
  assert(_raft_manager != nullptr);
  _is_leader = true;
  _raft_manager->on_leader_start(_group_id);
}

void SingleMachine::on_leader_stop(const butil::Status &status) {
  LOG(INFO) << "Node " << _node->node_id() << " stops to be leader";
  _is_leader = false;
}

void SingleMachine::on_start_following(
    const ::braft::LeaderChangeContext &ctx) {
  assert(_raft_manager != nullptr);
  _raft_manager->on_start_following(_group_id);
}

void *MulitGroupRaftManager::send_change_leader_req(void *machine) {
  CHECK(machine != nullptr);
  auto sm = static_cast<SingleMachine *>(machine);
  sm->request_leadership();
  return nullptr;
}

void MulitGroupRaftManager::coordinate_leader_if_need() {
  if (!_wait_for_coordinate) {
    // Only primary node should coordinate leader.
    return;
  }

  if (_election_done_count < _machines.size()) {
    // Wait until all the group's election done.
    return;
  }

  // Count the none-leader group.
  std::vector<SingleMachine *> none_leader_machines;
  for (auto &machine : _machines) {
    if (!machine->is_leader()) {
      none_leader_machines.emplace_back(machine.get());
    }
  }

  _coordinate_clousure.reset(
      new SynchronizedCountClosure(none_leader_machines.size()));
  for (auto &machine : none_leader_machines) {
    bthread_t tid;
    if (bthread_start_background(&tid, NULL, send_change_leader_req, machine) !=
        0) {
      LOG(ERROR) << "Fail to start bthread";
    }
  }
  // _coordinate_clousure->Run() be called in on_leader_start().
  _coordinate_clousure->wait();
  LOG(WARNING) << "Coordinate leader done.";

  _wait_for_coordinate = false;
  _coordinate_clousure.reset(nullptr);
}

void MulitGroupRaftManager::on_leader_start(int32_t group_id) {
  if (_wait_for_coordinate) {
    // Leader change cause by coordinate, do nothing.
    CHECK(_coordinate_clousure.get() != nullptr);
    LOG(WARNING) << "Group " << group_id << " leader change done.";
    _coordinate_clousure->Run();
    return;
  }
  _election_done_count++;
  if (group_id == 0) {
    // We do not deal with unexpecttable leader change, we assume that the net
    // work and all nodes are stable.
    // So the leader change only happens when we kill a node on purpose.
    // Which means the all the group only change leader once at a time.
    CHECK_EQ(_wait_for_coordinate, false);

    _wait_for_coordinate = true;
  }

  coordinate_leader_if_need();
}

void MulitGroupRaftManager::on_start_following(int32_t group_id) {
  if (_wait_for_coordinate) {
    // Leader change cause by coordinate, do nothing.
    return;
  }
  _election_done_count++;
  coordinate_leader_if_need();
}

} // namespace mbraft