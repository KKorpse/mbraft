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
#include <brpc/channel.h>
#include <brpc/controller.h>  // brpc::Controller
#include <brpc/server.h>      // brpc::Server
#include <bthread/bthread.h>
#include <gflags/gflags.h>  // DEFINE_*

#include <cassert>
#include <memory>
#include <vector>

#include "multi_group_raft_manager.h"

namespace mbraft {

#define LOG_WITH_PEER_ID(level) \
    LOG(level) << "[" << _group_idx << ": " << _peer_id.to_string() << "] "

DEFINE_int32(
    election_timeout_ms, 5000,
    "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(snapshot_interval, 72000, "Interval between each snapshot");

int SingleMachine::init(SingleMachineOptions &options) {
    CHECK(options.raft_manager != nullptr);

    // 1. Init Server (with 64 bytes alignment)
    void *mem = aligned_alloc(64, sizeof(brpc::Server));
    if (!mem) {
        LOG(ERROR) << "Fail to allocate memory for brpc::Server";
        return -1;
    }
    _server.reset(new (mem) brpc::Server());

    auto service = new SingleMachineServiceImpl(this);
    _server->AddService(service, brpc::SERVER_OWNS_SERVICE);

    if (braft::add_service(_server.get(), options.peer_id.addr) != 0) {
        LOG_WITH_PEER_ID(ERROR) << "Fail to add raft service";
        return -1;
    }

    if (_server->Start(options.peer_id.addr, NULL) != 0) {
        LOG_WITH_PEER_ID(ERROR) << "Fail to start Server";
        return -1;
    }

    // 2. Init Raft Node
    _raft_manager = options.raft_manager;

    braft::NodeOptions node_options;
    node_options.initial_conf = options.peers;
    node_options.election_timeout_ms = FLAGS_election_timeout_ms;
    node_options.fsm = this;
    node_options.node_owns_fsm = false;
    node_options.snapshot_interval_s = FLAGS_snapshot_interval;
    std::string prefix = "local://" + options.data_path;
    node_options.log_uri = prefix + "/LOG_WITH_PEER_ID";
    node_options.raft_meta_uri = prefix + "/raft_meta";
    node_options.snapshot_uri = prefix + "/snapshot";
    _node.reset(new braft::Node(options.group_id, options.peer_id));
    if (_node->init(node_options) != 0) {
        LOG_WITH_PEER_ID(ERROR) << "Fail to init raft node";
        _node.reset(nullptr);
        return -1;
    }

    return 0;
}

int SingleMachine::append_split_log(NewConfiguration &new_conf,
                                    braft::Closure *done) {
    std::string data = new_conf.serialize();
    UpperLog upper_log(UpperLog::LogType::SPLIT, data);
    int res = append_task(upper_log);
    if (res != 0) {
        LOG_WITH_PEER_ID(ERROR) << "Fail to append split log";
        return res;
    }

    return 0;
}

void SingleMachine::on_apply(braft::Iterator &iter) {
    for (; iter.valid(); iter.next()) {
        if (iter.done()) {
            braft::AsyncClosureGuard closure_guard(iter.done());
        }

        UpperLog upper_log;
        // FIXME: 实际数据能拷贝过来么？
        butil::IOBuf data_copy = iter.data();
        upper_log.deserialize_from(&data_copy);

        if (upper_log.get_type() == UpperLog::LogType::SPLIT) {
            assert(_raft_manager != nullptr);
            NewConfiguration new_conf;
            new_conf.deserialize_from(upper_log.get_data());

            // To distinguish the new address corresponding to the current node,
            // NewConfiguration uses a map for mapping, where the key is the
            // address of each node in the source raft group.
            braft::PeerId peer_id;
            if (new_conf.get_peer(_peer_id.to_string(), peer_id) != 0) {
                LOG_WITH_PEER_ID(ERROR) << "Fail to get peer";
                continue;
            }

            braft::Configuration conf = new_conf.to_braft_configuration();
            if (_raft_manager->add_raft_group(conf, peer_id) != 0) {
                LOG_WITH_PEER_ID(ERROR) << "Fail to add raft group";
                continue;
            }

        } else if (upper_log.get_type() == UpperLog::LogType::MERGE) {
            // TODO:
        } else if (upper_log.get_type() == UpperLog::LogType::NORMAL) {
            LOG(INFO) << "Apply normal log";
        } else {
            LOG_WITH_PEER_ID(ERROR)
                << "Unknown log type: " << upper_log.get_type();
        }

        LOG_WITH_PEER_ID(INFO)
            << "Node " << _node->node_id()
            << " Apply LOG_WITH_PEER_ID at index " << iter.index();
    }
}

void SingleMachine::on_snapshot_save(braft::SnapshotWriter *writer,
                                     braft::Closure *done) {
    LOG_WITH_PEER_ID(ERROR) << "Snapshot save not implemented";
    braft::AsyncClosureGuard closure_guard(done);
}

int SingleMachine::on_snapshot_load(braft::SnapshotReader *reader) {
    LOG_WITH_PEER_ID(ERROR) << "Snapshot load not implemented";
    return -1;
}

int SingleMachine::change_leader_to(braft::PeerId to) {
    int res = _node->transfer_leadership_to(to);
    if (res != 0) {
        LOG_WITH_PEER_ID(ERROR)
            << "Fail to transfer leader to " << to.to_string();
        return res;
    }
    return 0;
}

int SingleMachine::request_leadership() {
    CHECK(!_is_leader);
    CHECK(_node != nullptr);
    auto leader = _node->leader_id();
    CHECK(!leader.is_empty());

    int res{0};

    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller());
    std::unique_ptr<LeaderChangeRequest> request(new LeaderChangeRequest);
    std::unique_ptr<LeaderChangeResponse> response(new LeaderChangeResponse);
    request->set_change_to(_node->node_id().peer_id.to_string());

    brpc::Channel channel;
    brpc::ChannelOptions channel_opt;
    channel_opt.connect_timeout_ms = 1000;
    channel_opt.timeout_ms = -1;  // We don't need RPC timeout
    res = channel.Init(leader.addr, &channel_opt);
    if (res != 0) {
        LOG_WITH_PEER_ID(ERROR) << "Fail to init sending channel: "
                                << _node->node_id().peer_id.to_string()
                                << " to " << leader.to_string();
        return res;
    }

    SingleMachineService_Stub leader_stub(&channel);
    LOG_WITH_PEER_ID(WARNING)
        << _node->node_id().peer_id.to_string() << " Request leadership from "
        << leader.to_string();
    leader_stub.leader_change(cntl.get(), request.get(), response.get(),
                              nullptr);
    if (cntl->Failed()) {
        LOG_WITH_PEER_ID(ERROR)
            << "Fail to request leadership from " << leader.to_string() << " : "
            << cntl->ErrorText();
        return -1;
    }

    return 0;
}

void SingleMachine::on_leader_start(int64_t term) {
    LOG_WITH_PEER_ID(INFO) << "Node " << _node->node_id()
                           << " starts to be leader";
    assert(_raft_manager != nullptr);
    _is_leader = true;
    _raft_manager->on_leader_start(_group_idx);
}

void SingleMachine::on_leader_stop(const butil::Status &status) {
    LOG_WITH_PEER_ID(INFO) << "Node " << _node->node_id()
                           << " stops to be leader";
    _is_leader = false;
    _raft_manager->on_leader_stop(_group_idx);
}

void SingleMachine::on_shutdown() {
    LOG_WITH_PEER_ID(INFO) << "Node " << _node->node_id() << " shutdown";
}

void SingleMachine::on_error(const ::braft::Error &e) {
    LOG_WITH_PEER_ID(ERROR) << "Node " << _node->node_id() << " error: " << e;
}
void SingleMachine::on_configuration_committed(
    const ::braft::Configuration &conf) {
    LOG_WITH_PEER_ID(ERROR) << "Configuration committed not implemented";
}

void SingleMachine::on_stop_following(const ::braft::LeaderChangeContext &ctx) {
    LOG_WITH_PEER_ID(INFO) << "Node " << _node->node_id() << " stops following "
                           << ctx;
}

void SingleMachine::on_start_following(
    const ::braft::LeaderChangeContext &ctx) {
    assert(_raft_manager != nullptr);
    _raft_manager->on_start_following(_group_idx);
}

}  // namespace mbraft