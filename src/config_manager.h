#pragma once
#include <bits/types/FILE.h>
#include <butil/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>
// Add for config
#include <braft/configuration.h>
#include <braft/log_entry.h>

#include <fstream>
#include <iostream>
#include <string>

#include "json.hpp"

namespace mbraft {

class Database;
class DataTable;
class Tuple;

// Configuration file format:

/*
{
    "node_number": 3, // number of nodes in cluster
    "stream_number": 2, // number of log streams
    "group_id": 666, // cluster common id
    "name": "node1", // name of current node
     // 节点配置数量必须和 node number 一致
    "node_configs":[
        {
            // 地址数量必须和 stream number 一致，可读取为字符串，用
braft::PeerId 解析 "name": "node1", "adresses": ["127.0.0.1:8001",
"127.0.0.1:8011"], "origin_priority": 1,
        },
        {
            "name": "node2",
            "adresses": ["127.0.0.2:8002", "127.0.0.2:8012"],
            "origin_priority": 2,
        },
        {
            "name": "node3",
            "adresses": ["127.0.0.2:8003", "127.0.0.2:8013"],
            "origin_priority": 3,
        },
    ],
}
*/

struct NodeConfiguration {
    std::vector<braft::PeerId> addresses;
    int64_t origin_priority;
};

struct ConfigurationManagerOptions {
    ConfigurationManagerOptions() = default;
    ~ConfigurationManagerOptions() = default;

    std::string config_file_path;
    Database *db;
};

// Manager the history of configuration changing
class ConfigurationManager {
   public:
    ConfigurationManager() {}
    ~ConfigurationManager() {}

    // 1. Set the configuration file path.
    // 2. Load the configuration from the file.
    // 3. Try to load the configuration from table in memory. (这个以后再实现)
    butil::Status init(ConfigurationManagerOptions &&options);

    // load the configuration from the file. Initialize this class' member.
    // the configuration from file has less origin_priority than the
    // configuration from log storage.
    // 1. used at the cluster bootstrap.
    butil::Status load_config_from_file();

    // load the configuration from the memory.
    // there is a special table used to store the configuration.
    // 1. get the configuration table,
    // 2. read all the tuple from the table,
    // 3. construct the configuration from the tuple.
    void load_config_from_memory();

    // For member change.
    // Our member change strategy is single member change at a time, so the
    // configure manager need to support single member change only.
    // @ids: the size of ids must be the same as the number of streams. For each
    //      id in is, must insert to each ClusterConfiguration.
    // @retrun: If the size not right, duplicate ids or other error, return
    //      false.
    bool add_one_peer(std::string &name, NodeConfiguration &node_config);
    bool remove_one_peer(std::string &name);

    // Return the info of the configuration manager.
    // eg. all the member of this class.
    std::string to_string();

    // Get the configuration at |index|
    braft::Configuration get_config_at_index(int64_t index) const;

    int64_t get_stream_nums() const { return _stream_number; }

    int64_t get_node_number() const { return _node_number; }

    braft::GroupId get_group_id() const { return _group_id; }

    std::string get_name() const { return _name; }

    bool check_if_exist_in_config(std::string &name) {
        return _node_configs.count(name) != 0;
    }

    std::string check_delta_config(std::vector<std::string> new_names) {
        for (auto it : _node_configs) {
            if (std::find(new_names.begin(), new_names.end(), it.first) ==
                new_names.end()) {
                return it.first;
            }
        }
        return "";
    }

    // Get the server ids of current node.
    // Each stream has a server id.
    std::vector<braft::PeerId> get_server_ids() const {
        LOG(INFO) << "get_server_ids: "
                  << _node_configs.at(_name).addresses.size();
        return _node_configs.at(_name).addresses;
    }

    std::string get_serialized_config();

    // Used to get the server ids of other nodes.
    // To update LogStream's leader_id.
    // Any better way?
    std::vector<braft::PeerId> get_server_ids_by_name(std::string name) const {
        LOG(INFO) << "get_server_ids: "
                  << _node_configs.at(name).addresses.size();
        return _node_configs.at(name).addresses;
    }

    std::vector<std::string> get_server_ids_by_name_in_str(
        std::string name) const {
        std::vector<std::string> addr_str;
        for (auto iter : _node_configs.at(name).addresses) {
            addr_str.push_back(iter.to_string());
        }
        return addr_str;
    }

    std::vector<braft::PeerId> get_server_ids_of_election() {
        std::vector<braft::PeerId> election_peers;
        for (auto iter : _node_configs) {
            election_peers.push_back(iter.second.addresses.at(0));
        }
        return election_peers;
    }

    std::unordered_map<std::string, braft::PeerId>
    get_server_name_id_map_of_election() {
        std::unordered_map<std::string, braft::PeerId> name_id_map;
        for (auto iter : _node_configs) {
            name_id_map[iter.first] = iter.second.addresses.at(0);
        }
        return name_id_map;
    }

    std::vector<braft::PeerId> get_all_server_ids() {
        std::vector<braft::PeerId> all_peers;
        for (auto iter : _node_configs) {
            for (auto peer : iter.second.addresses) {
                all_peers.push_back(peer);
            }
        }
        return all_peers;
    }

    int64_t get_priority_by_name(std::string name) const {
        return _node_configs.at(name).origin_priority;
    }

    std::vector<braft::PeerId> get_all_leader_id_by_peer_id(
        braft::PeerId peer_id) {
        for (auto iter : _node_configs) {
            if (std::find(iter.second.addresses.begin(),
                          iter.second.addresses.end(),
                          peer_id) != iter.second.addresses.end()) {
                return iter.second.addresses;
            }
        }
        LOG(ERROR) << "didn't find leader id!";
        return std::vector<braft::PeerId>{};
    }

    bool check_address(const std::string address) {
        if (braft::PeerId().parse(address) == -1) {
            return false;
        }
        return true;
    }

    // const ConfigurationEntry& last_configuration() const;
   private:
    // Load config from code file, only used for test.
    void _load_default_config();

   private:
    std::string _name;          // name of current node
    int64_t _node_number{0};    // number of nodes in cluster
    int64_t _stream_number{0};  // number of log streams
    braft::GroupId _group_id;   // cluster common id

    // configurations of all nodes
    std::unordered_map<std::string,  // name
                       NodeConfiguration>
        _node_configs;

    // Each ClusterConfiguration is a set of peers' endpoints.
    // For each stream there is a ClusterConfiguration.
    // So the size of _configurations is the same as the number of streams.
    // std::vector<std::unique_ptr<ClusterConfiguration>> _configurations;

    // The configuration file path.
    std::string _config_file_path;

    // Used to implement the load_config_from_memory() func.
    Database *_db;
};

}  // namespace mbraft