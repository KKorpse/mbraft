#include <braft/configuration.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

#include <cstddef>
#include <string>

#include "config_manager.h"

namespace mbraft {

DEFINE_bool(raft_use_default_config, false, "use the default config for test");

DEFINE_string(raft_default_node_name, "", "the default name of current node");

// if it's empty string, use the default config
// if it's not empty, use it rather than the Options.config_file_path
DEFINE_string(raft_config_path, "", "the default path of current config file");

void ConfigurationManager::_load_default_config() {
    _node_number = 3;
    _stream_number = 2;
    _group_id = "666";

    if (_name.empty()) {
        _name = FLAGS_raft_default_node_name;
        CHECK(!_name.empty());
        LOG(WARNING) << "Use default peer name: " << _name;
    }

    // node0
    NodeConfiguration node_config0;
    node_config0.addresses.push_back(braft::PeerId("127.0.0.1:8000"));
    node_config0.addresses.push_back(braft::PeerId("127.0.0.1:8001"));
    node_config0.origin_priority = 1;
    _node_configs.insert(std::make_pair("node0", node_config0));

    // node1
    NodeConfiguration node_config1;
    node_config1.addresses.push_back(braft::PeerId("127.0.0.1:8010"));
    node_config1.addresses.push_back(braft::PeerId("127.0.0.1:8011"));
    node_config1.origin_priority = 2;
    _node_configs.insert(std::make_pair("node1", node_config1));

    // node2
    NodeConfiguration node_config2;
    node_config2.addresses.push_back(braft::PeerId("127.0.0.1:8020"));
    node_config2.addresses.push_back(braft::PeerId("127.0.0.1:8021"));
    node_config2.origin_priority = 3;
    _node_configs.insert(std::make_pair("node2", node_config2));
}

butil::Status ConfigurationManager::init(
    ConfigurationManagerOptions&& options) {
    if (FLAGS_raft_use_default_config) {
        LOG(INFO) << "Use default config";
        _load_default_config();
        return butil::Status::OK();
    }
    _config_file_path = options.config_file_path;
    if (!FLAGS_raft_config_path.empty()) {
        _config_file_path = FLAGS_raft_config_path;
    }
    CHECK(!_config_file_path.empty());

    return load_config_from_file();
}

butil::Status ConfigurationManager::load_config_from_file() {
    // 1.Load config file
    std::ifstream config_file(_config_file_path);
    if (!config_file.is_open()) {
        LOG(ERROR) << "Fail to open file: " << _config_file_path << std::endl;
        return butil::Status(EINVAL, "Fail to open file");
    }

    // 2.Open File
    nlohmann::json config;
    config_file >> config;
    if (config.is_null()) {
        LOG(ERROR) << "Fail to load json file or empty json file" << std::endl;
        return butil::Status(EINVAL,
                             "Fail to load json file or empty json file");
    }
    config_file.close();

    // 3.Load members from file
    try {
        _name = config["name"];
        _node_number = config["node_number"];
        _stream_number = config["stream_number"];
        _group_id = config["group_id"];
        // Load node configs from json file
        nlohmann::json node_configs = config["node_configs"];
        for (const nlohmann::json& node_config : node_configs) {
            // Parse node_name, address and origin_priority
            std::string node_name = node_config["name"];
            std::vector<std::string> string_addresses =
                node_config["addresses"];
            // Convert address to braft::peerid
            std::vector<braft::PeerId> addresses;
            for (const std::string& address : string_addresses) {
                if (!check_address(address)) {
                    LOG(ERROR) << "Address can't parse";
                    return butil::Status(EINVAL, "Address can't parse");
                }
                addresses.push_back(braft::PeerId(address));
            }
            int priority = node_config["priority"];

            _node_configs[node_name].origin_priority = priority;
            _node_configs[node_name].addresses = addresses;
        }
    } catch (const nlohmann::json::exception& e) {
        LOG(ERROR) << "JSON parse failed " << e.what() << std::endl;
        return butil::Status(EINVAL, "JSON parse failed");
    }

    if (!FLAGS_raft_default_node_name.empty()) {
        _name = FLAGS_raft_default_node_name;
        LOG(INFO) << "Use gflags peer name: " << _name;
    }

    return butil::Status::OK();
}

bool ConfigurationManager::add_one_peer(std::string& name,
                                        NodeConfiguration& node_config) {
    auto& ids = node_config.addresses;
    CHECK(ids.size() == (size_t)_stream_number);
    CHECK(_node_configs.find(name) == _node_configs.end());
    _node_configs.insert(std::make_pair(name, node_config));
    _node_number += 1;
    return true;
}

bool ConfigurationManager::remove_one_peer(std::string& name) {
    // Delete node configure by name from both _node_configs and
    // _configurations.
    if (_node_configs.find(name) == _node_configs.end()) {
        LOG(FATAL) << "The node not exist";
        return false;
    }
    _node_configs.erase(name);
    _node_number -= 1;
    return true;
}

std::string ConfigurationManager::to_string() {
    std::string result;
    result += "\nCurrent node : " + _name + "\n" + "group : " + _group_id +
              "\n" +
              "number of node in cluster: " + std::to_string(_node_number) +
              "\n"
              "number of log streams: " +
              std::to_string(_stream_number) + "\n";
    result += "Cluster information: \n";
    for (const auto& pair : _node_configs) {
        std::string key = pair.first;
        NodeConfiguration value = pair.second;
        result += "Node name: " + key + ", Node origin_priority: " +
                  std::to_string(value.origin_priority);
        result += ", Node address: ";
        for (const braft::PeerId& id : value.addresses) {
            result += id.to_string() + " ";
        }
        result += "]\n";
    }
    return result;
}

braft::Configuration ConfigurationManager::get_config_at_index(
    int64_t index) const {
    if (index > _stream_number) {
        return braft::Configuration();
    }
    std::vector<braft::PeerId> peer_vector;
    for (const auto& pair : _node_configs) {
        std::string key = pair.first;
        NodeConfiguration value = pair.second;
        peer_vector.push_back(value.addresses[index]);
    }

    return braft::Configuration(peer_vector);
}

std::string ConfigurationManager::get_serialized_config() {
    nlohmann::json config;
    config["name"] = _name;
    config["node_number"] = _node_number;
    config["stream_number"] = _stream_number;
    config["group_id"] = _group_id;

    nlohmann::json node_configs;
    for (auto item : _node_configs) {
        nlohmann::json tmp;
        std::vector<std::string> addr_strs;
        for (auto addr : item.second.addresses) {
            addr_strs.push_back(addr.to_string());
        }
        tmp["addresses"] = addr_strs;
        tmp["name"] = item.first;
        node_configs.push_back(tmp);
    }
    config["node_configs"] = node_configs;
    return config.dump();
}

}  // namespace mbraft