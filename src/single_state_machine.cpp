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

#include "multi_group_raft_manager.h"
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

namespace mbraft {

DEFINE_int32(
    election_timeout_ms, 5000,
    "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(snapshot_interval, 7200, "Interval between each snapshot");

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

void SingleMachine::on_apply(braft::Iterator &iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard closure_guard(iter.done());

    LOG(INFO) << "Node " << _node->node_id() << " Apply log at index "
              << iter.index();
  }
}

void SingleMachine::on_snapshot_save(braft::SnapshotWriter *writer,
                                     braft::Closure *done) {
  LOG(ERROR) << "Snapshot save not implemented";
  braft::AsyncClosureGuard closure_guard(done);
}

int SingleMachine::on_snapshot_load(braft::SnapshotReader *reader) {
  LOG(ERROR) << "Snapshot load not implemented";
  return -1;
}

int SingleMachine::change_leader_to(braft::PeerId to) {
  int res = _node->transfer_leadership_to(to);
  if (res != 0) {
    LOG(ERROR) << "Fail to transfer leader to " << to.to_string();
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
  channel_opt.timeout_ms = -1; // We don't need RPC timeout
  res = channel.Init(leader.addr, &channel_opt);
  if (res != 0) {
    LOG(ERROR) << "Fail to init sending channel: "
               << _node->node_id().peer_id.to_string() << " to "
               << leader.to_string();
    return res;
  }

  MbraftService_Stub leader_stub(&channel);
  LOG(WARNING) << _node->node_id().peer_id.to_string()
               << " Request leadership from " << leader.to_string();
  // TODO: impl of leader_change
  leader_stub.leader_change(cntl.get(), request.get(), response.get(), nullptr);
  if (cntl->Failed()) {
    LOG(ERROR) << "Fail to request leadership from " << leader.to_string()
               << " : " << cntl->ErrorText();
    return -1;
  }

  return 0;
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

void SingleMachine::on_shutdown() {
  LOG(INFO) << "Node " << _node->node_id() << " shutdown";
}

void SingleMachine::on_error(const ::braft::Error &e) {
  LOG(ERROR) << "Node " << _node->node_id() << " error: " << e;
}
void SingleMachine::on_configuration_committed(
    const ::braft::Configuration &conf) {
  LOG(ERROR) << "Configuration committed not implemented";
}

void SingleMachine::on_stop_following(const ::braft::LeaderChangeContext &ctx) {
  LOG(INFO) << "Node " << _node->node_id() << " stops following " << ctx;
}

void SingleMachine::on_start_following(
    const ::braft::LeaderChangeContext &ctx) {
  assert(_raft_manager != nullptr);
  _raft_manager->on_start_following(_group_id);
}

} // namespace mbraft