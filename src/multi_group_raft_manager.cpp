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
#include <cstddef>
#include <gflags/gflags.h> // DEFINE_*
#include <memory>
#include <vector>

#include "mbraft.pb.h"
#include "multi_group_raft_manager.h"

namespace mbraft {
// TODO:
void MulitGroupRaftManager::init_and_start(
    MulitGroupRaftManagerOptions &options) {
  CHECK_GE(options.group_count, 0);
  auto server_ids = _config_manager.get_server_ids();
  for (int32_t idx = 0; idx < options.group_count; ++idx) {

    SingleMachineOptions machine_options;
    machine_options.raft_manager = this;
    machine_options.peers = _config_manager.get_config_at_index(idx);
    machine_options.group_idx = idx;
    machine_options.addr = server_ids[idx];
    machine_options.group_id = _config_manager.get_group_id();
    _machines.emplace_back(new SingleMachine);
    _machines.back().machine->init(machine_options);
  }
}

void *MulitGroupRaftManager::send_change_leader_req(void *machine) {
  CHECK(machine != nullptr);
  auto sm = static_cast<SingleMachine *>(machine);
  sm->request_leadership();
  return nullptr;
}

void MulitGroupRaftManager::on_leader_start(int32_t group_id) {
  std::lock_guard<std::mutex> lock(_cood_mutex);
  _machines[group_id].state = LEADER;
  if (group_id == 0) {
    // We do not deal with unexpecttable leader change, we assume that the net
    // work and all nodes are stable.
    // So the leader change only happens when we kill a node on purpose.
    // Which means the all the group only change leader once at a time.
    if (_is_coordinating()) {
      LOG(ERROR) << "Leader change during coordinate leader, this should not "
                    "happen when testing.";
      return;
    }

    // Start to coordinate leader.
    LOG(INFO) << "FLAG: Start to coordinate leader.";
    _state = COORDINATING;
    _needed_count = 0;
    for (auto &machine : _machines) {
      if (machine.state == LEADER || machine.state == LEADER_CHANGING) {
        continue;
      }
      ++_needed_count;
      if (machine.state == FOLLOWER) {
        CHECK_EQ(_start_leader_change(group_id), 0) << "Fail to start leader "
                                                       "change.";
      }
    }

  } else {
    LOG(INFO) << "Group " << group_id << " starts to be leader.";
    if (!_is_coordinating()) {
      // 1. The coordinating is not start yet.
      // 2. or this node is not coordinating node. (The group[0]'s leader
      // node).
      LOG(INFO) << "group election down, but Not in coordinating or this node "
                   "is not coordinating node.";
      return;
    }

    // Count down the needed count.
    CHECK_GT(_needed_count, 0) << "The needed count should be greater than 0.";
    --_needed_count;
    if (_needed_count == 0) {
      LOG(INFO) << "FLAG: All group election done, finish coordinating.";
      _state = LEADING;
    }
  }
}

int MulitGroupRaftManager::on_start_following(int32_t group_id) {
  if ((size_t)group_id >= _machines.size()) {
    LOG(ERROR) << "Invalid group id: " << group_id;
    return -1;
  }

  std::lock_guard<std::mutex> lock(_cood_mutex);
  _machines[group_id].state = FOLLOWER;

  if (!_is_coordinating()) {
    // 1. The coordinating is not start yet.
    // 2. or this node is not coordinating node. (The group[0]'s leader
    // node).
    LOG(INFO) << "group election down, but Not in coordinating or this node "
                 "is not coordinating node.";
    return -1;

  } else {
    // Coordinating is in progress, so we need to change leader to this node.
    return _start_leader_change(group_id);
  }
}

int MulitGroupRaftManager::_start_leader_change(int32_t group_id) {
  CHECK_LT(group_id, _machines.size());
  bthread_t tid;
  auto res = bthread_start_background(&tid, NULL, send_change_leader_req,
                                      _machines[group_id].machine.get());
  if (res != 0) {
    LOG(ERROR) << "Fail to start bthread, res: " << res;
    return res;
  }
  _machines[group_id].state = LEADER_CHANGING;

  return 0;
}
} // namespace mbraft
