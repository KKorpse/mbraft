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

#include <braft/protobuf_file.h>  // braft::ProtoBufFile
#include <braft/raft.h>           // braft::Node braft::StateMachine
#include <braft/storage.h>        // braft::SnapshotWriter
#include <braft/util.h>           // braft::AsyncClosureGuard
#include <brpc/channel.h>
#include <brpc/controller.h>  // brpc::Controller
#include <brpc/server.h>      // brpc::Server
#include <bthread/bthread.h>
#include <gflags/gflags.h>  // DEFINE_*

#include <cassert>
#include <cstddef>
#include <memory>
#include <vector>

#include "mbraft.pb.h"
#include "multi_group_raft_manager.h"

#define LOG_WITH_NAME(level) LOG(level) << "[" << _name << "] "

namespace mbraft {
// TODO:
void MulitGroupRaftManager::init_and_start(
    MulitGroupRaftManagerOptions &options) {
    CHECK_GE(options.group_count, 0);
    CHECK_EQ(options.group_count, options.configs.size());
    CHECK_EQ(options.group_count, options.server_ids.size());
    _name = options.name;

    for (int32_t idx = 0; idx < options.group_count; ++idx) {
        SingleMachineOptions machine_options;
        machine_options.raft_manager = this;
        machine_options.peers = options.configs[idx];
        machine_options.group_idx = idx;
        machine_options.peer_id = options.server_ids[idx];
        machine_options.group_id = _config_manager.get_group_id();
        _machines.emplace_back(new SingleMachine);
        _machines.back().machine->init(machine_options);
    }
}

int MulitGroupRaftManager::split_raft_group(int32_t group_idx,
                                            braft::Configuration &conf,
                                            NewConfiguration &new_conf) {
    CHECK_LT(group_idx, _machines.size());
    if (!_is_coordinating()) {
        LOG_WITH_NAME(ERROR) << "This node is not coordinating leader change.";
        return -1;
    }
    if (!_is_all_leader_on_this_node()) {
        LOG_WITH_NAME(ERROR) << "All leader should be on this "
                                "node when split group.";
        return -1;
    }

    int res = _machines[group_idx].machine->append_split_log(new_conf);
    if (res != 0) {
        LOG_WITH_NAME(ERROR) << "Fail to append split log";
        return res;
    }

    LOG_WITH_NAME(WARNING) << "FLAG: Start to split group.";
    return 0;
}

int MulitGroupRaftManager::merge_raft_group(int32_t source_group_id,
                                            int32_t target_group_id) {
    std::unique_lock<std::mutex> lock(_mutex);
    CHECK((size_t)source_group_id < _machines.size());
    CHECK((size_t)target_group_id < _machines.size());
    if (_is_coordinating()) {
        LOG_WITH_NAME(ERROR) << "This node is coordinating leader change.";
        return -1;
    }

    if (_is_leading()) {
        LOG_WITH_NAME(ERROR) << "This node is leader node.";
        return -1;
    }

    _target_group_id = target_group_id;

    // Stop the source_group
    bool is_source = true;
    auto res = _machines[source_group_id].machine->append_merge_log(is_source);
    if (res != 0) {
        LOG_WITH_NAME(ERROR) << "Fail to append merge log";
        return res;
    }

    return 0;
}

int MulitGroupRaftManager::add_raft_group(braft::Configuration &conf,
                                          braft::PeerId peer_id) {
    std::unique_lock<std::mutex> lock(_mutex);

    if (_is_coordinating()) {
        LOG_WITH_NAME(ERROR) << "This node is coordinating leader change.";
        return -1;
    }

    SingleMachineOptions machine_options;
    machine_options.raft_manager = this;
    machine_options.peers = conf;
    machine_options.group_idx = _machines.size();
    machine_options.peer_id = peer_id;
    machine_options.group_id = _config_manager.get_group_id();
    _machines.emplace_back(new SingleMachine);
    int res = _machines.back().machine->init(machine_options);
    if (res != 0) {
        LOG_WITH_NAME(ERROR) << "Fail to init SingleMachine";
        return res;
    }

    return 0;
}

int MulitGroupRaftManager::on_merge_source_apply() {
    std::unique_lock<std::mutex> lock(_mutex);
    if (_target_group_id == INVALIED_GROUP_IDX) {
        LOG_WITH_NAME(ERROR) << "Invalid target group id when merge!";
        return -1;
    }

    if (!_is_leading()) {
        LOG_WITH_NAME(WARNING) << "This node is not leader node.";
        return 0;
    }

    CHECK((size_t)_target_group_id < _machines.size());
    bool is_source = false;
    auto res = _machines[_target_group_id].machine->append_merge_log(is_source);
    if (res != 0) {
        LOG_WITH_NAME(ERROR) << "Fail to append merge log";
        return res;
    }

    return 0;
}

int MulitGroupRaftManager::on_merge_target_apply() {
    std::unique_lock<std::mutex> lock(_mutex);

    if (!_is_leading()) {
        LOG_WITH_NAME(WARNING) << "This node is not leader node.";
        return 0;
    }

    _target_group_id = INVALIED_GROUP_IDX;
    LOG(WARNING) << "FLAG: Merge target group done.";
    
    return 0;
}

void *MulitGroupRaftManager::send_change_leader_req(void *machine) {
    CHECK(machine != nullptr);
    auto sm = static_cast<SingleMachine *>(machine);
    sm->request_leadership();
    return nullptr;
}

void MulitGroupRaftManager::on_leader_start(int32_t group_idx) {
    std::lock_guard<std::mutex> lock(_mutex);
    _machines[group_idx].state = LEADER;
    LOG_WITH_NAME(INFO) << "Group " << group_idx << " starts to be leader.";

    if (group_idx == 0) {
        // We do not deal with unexpecttable leader change, we assume that the
        // net work and all nodes are stable. So the leader change only happens
        // when we kill a node on purpose. Which means the all the group only
        // change leader once at a time.
        if (_is_coordinating()) {
            LOG_WITH_NAME(ERROR)
                << "Leader change during coordinate leader, this should not "
                   "happen when testing.";
            return;
        }

        // Start to coordinate leader.
        LOG_WITH_NAME(INFO) << "FLAG: Start to coordinate leader.";
        _state = COORDINATING;
        _needed_count = 0;
        for (auto &machine : _machines) {
            if (machine.state == LEADER || machine.state == LEADER_CHANGING) {
                continue;
            }
            ++_needed_count;
            if (machine.state == FOLLOWER) {
                CHECK_EQ(_start_leader_change(group_idx), 0)
                    << "Fail to start leader "
                       "change.";
            }
        }

        return;
    }

    if (!_is_coordinating()) {
        // 0. This raft group the new group by spliting.
        if (_state == LEADING) {
            LOG_WITH_NAME(WARNING)
                << "FLAG: new group leader start, and no need to "
                   "coordinate.";
            return;
        }

        // 1. The coordinating is not start yet.
        // 2. or this node is not coordinating node. (The group[0]'s leader
        // node).
        LOG_WITH_NAME(WARNING)
            << "group election down, but Not in coordinating or this node "
               "is not coordinating node.";

        return;
    }

    // Count down the needed count.
    CHECK_GT(_needed_count, 0) << "The needed count should be greater than 0.";
    --_needed_count;
    if (_needed_count == 0) {
        LOG_WITH_NAME(INFO)
            << "FLAG: All group election done, finish coordinating.";
        _state = LEADING;
    }
}

int MulitGroupRaftManager::on_start_following(int32_t group_idx) {
    if ((size_t)group_idx >= _machines.size()) {
        LOG_WITH_NAME(ERROR) << "Invalid group id: " << group_idx;
        return -1;
    }

    std::lock_guard<std::mutex> lock(_mutex);
    _machines[group_idx].state = FOLLOWER;

    if (!_is_coordinating()) {
        // 0. This raft group the new group by spliting.
        if (_state == LEADING) {
            LOG_WITH_NAME(WARNING)
                << "new group election done, need to coordinate.";
            return _start_leader_change(group_idx);
        }

        // 1. The coordinating is not start yet.
        // 2. or this node is not coordinating node. (The group[0]'s leader
        // node).
        LOG_WITH_NAME(INFO)
            << "group election down, but Not in coordinating or this node "
               "is not coordinating node.";
        return -1;

    } else {
        // Coordinating is in progress, so we need to change leader to this
        // node.
        return _start_leader_change(group_idx);
    }
}

int MulitGroupRaftManager::_start_leader_change(int32_t group_idx) {
    CHECK_LT(group_idx, _machines.size());
    bthread_t tid;
    auto res = bthread_start_background(&tid, NULL, send_change_leader_req,
                                        _machines[group_idx].machine.get());
    if (res != 0) {
        LOG_WITH_NAME(ERROR) << "Fail to start bthread, res: " << res;
        return res;
    }
    _machines[group_idx].state = LEADER_CHANGING;

    return 0;
}

int MulitGroupRaftManager::on_leader_stop(int32_t group_idx) {
    CHECK_LT(group_idx, _machines.size());
    _machines[group_idx].state = ELECTION;
    return 0;
}

bool MulitGroupRaftManager::_is_all_leader_on_this_node() {
    for (auto &machine : _machines) {
        if (machine.state != LEADER) {
            return false;
        }
    }
    return true;
}

}  // namespace mbraft
