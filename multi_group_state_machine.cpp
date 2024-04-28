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
#include <brpc/controller.h>     // brpc::Controller
#include <brpc/server.h>         // brpc::Server
#include <gflags/gflags.h>       // DEFINE_*

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
  node_options.election_timeout_ms = FLAGS_election_timeout_ms;
  node_options.snapshot_interval_s = FLAGS_snapshot_interval;
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
  int32_t none_leader_count = 0;
  for (auto &machine : _machines) {
    if (!machine.is_leader()) {
      none_leader_count++;
    }
  }

  _coordinate_clousure.reset(new SynchronizedCountClosure(none_leader_count));
  // TODO: do the leader change.
  _coordinate_clousure->wait();
  LOG(WARNING) << "Coordinate leader done.";
}

void MulitGroupRaftManager::on_leader_start(int32_t group_id) {
  if (_wait_for_coordinate) {
    // Leader change cause by coordinate, do nothing.
    return;
  }
  _election_done_count++;
  if (group_id == 0) {
    // We do not deal with unexpecttable leader change, we assume that the net
    // work and all nodes are stable.
    // So the leader change only happens when we kill a node on purpose.
    // Which means the all the group only change leader once at a time.
    CHECK_EQ(_wait_for_coordinate, false);

    // TODO: Should change all the other group's leader to this node.
    // At first we should wait until other nodes' election done.
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