#include <braft/configuration.h>
#include <braft/protobuf_file.h>  // braft::ProtoBufFile
#include <braft/raft.h>           // braft::Node braft::StateMachine
#include <braft/storage.h>        // braft::SnapshotWriter
#include <braft/util.h>           // braft::AsyncClosureGuard
#include <brpc/controller.h>      // brpc::Controller
#include <brpc/server.h>          // brpc::Server
#include <gflags/gflags.h>        // DEFINE_*

namespace mbraft {
class NewConfiguration {
   public:
    typedef std::unordered_map<std::string, braft::PeerId>::const_iterator
        const_iterator;

    // Construct an empty configuration.
    NewConfiguration() {}

    // Construct from peers stored in std::vector.
    explicit NewConfiguration(
        const std::vector<std::pair<std::string, braft::PeerId>>& peers) {
        for (const auto& peer : peers) {
            _peers[peer.first] = peer.second;
        }
    }

    // Construct from peers stored in std::unordered_map
    explicit NewConfiguration(
        const std::unordered_map<std::string, braft::PeerId>& peers)
        : _peers(peers) {}

    // Assign from peers stored in std::vector
    void operator=(
        const std::vector<std::pair<std::string, braft::PeerId>>& peers) {
        _peers.clear();
        for (const auto& peer : peers) {
            _peers[peer.first] = peer.second;
        }
    }

    // Assign from peers stored in std::unordered_map
    void operator=(
        const std::unordered_map<std::string, braft::PeerId>& peers) {
        _peers = peers;
    }

    // Remove all peers.
    void reset() { _peers.clear(); }

    bool empty() const { return _peers.empty(); }
    size_t size() const { return _peers.size(); }

    const_iterator begin() const { return _peers.begin(); }
    const_iterator end() const { return _peers.end(); }

    // Clear the container and put peers in.
    void list_peers(
        std::unordered_map<std::string, braft::PeerId>* peers) const {
        peers->clear();
        *peers = _peers;
    }

    void list_peers(
        std::vector<std::pair<std::string, braft::PeerId>>* peers) const {
        peers->clear();
        peers->reserve(_peers.size());
        for (const auto& peer : _peers) {
            peers->push_back(peer);
        }
    }

    void append_peers(std::unordered_map<std::string, braft::PeerId>* peers) {
        peers->insert(_peers.begin(), _peers.end());
    }

    // Add a peer.
    // Returns true if the peer is newly added.
    bool add_peer(const std::string& key, const braft::PeerId& peer) {
        return _peers.emplace(key, peer).second;
    }

    // Remove a peer.
    // Returns true if the peer is removed.
    bool remove_peer(const std::string& key) { return _peers.erase(key); }

    // True if the peer exists.
    bool contains(const std::string& key) const {
        return _peers.find(key) != _peers.end();
    }

    // True if ALL peers exist.
    bool contains(const std::vector<std::string>& keys) const {
        return std::all_of(keys.begin(), keys.end(),
                           [this](const std::string& key) {
                               return _peers.find(key) != _peers.end();
                           });
    }

    // True if peers are same.
    bool equals(
        const std::vector<std::pair<std::string, braft::PeerId>>& peers) const {
        std::unordered_map<std::string, braft::PeerId> peer_map;
        for (const auto& peer : peers) {
            if (_peers.find(peer.first) == _peers.end()) {
                return false;
            }
            peer_map[peer.first] = peer.second;
        }
        return peer_map.size() == _peers.size();
    }

    bool equals(const NewConfiguration& rhs) const {
        return _peers == rhs._peers;
    }

    // Get the difference between |*this| and |rhs|
    // |included| would be assigned to |*this| - |rhs|
    // |excluded| would be assigned to |rhs| - |*this|
    void diffs(const NewConfiguration& rhs, NewConfiguration* included,
               NewConfiguration* excluded) const {
        *included = *this;
        *excluded = rhs;
        for (const auto& peer : _peers) {
            excluded->_peers.erase(peer.first);
        }
        for (const auto& peer : rhs._peers) {
            included->_peers.erase(peer.first);
        }
    }

    // Serialize peers to string
    std::string serialize() const {
        std::ostringstream oss;
        for (const auto& peer : _peers) {
            oss << peer.first << "," << peer.second.to_string() << ";";
        }
        return oss.str();
    }

    // Deserialize peers from string
    int deserialize_from(const std::string& str) {
        _peers.clear();
        std::istringstream iss(str);
        std::string item;
        while (std::getline(iss, item, ';')) {
            if (item.empty()) continue;
            size_t pos = item.find(',');
            if (pos == std::string::npos) {
                return -1;  // Parsing error
            }
            std::string key = item.substr(0, pos);
            braft::PeerId peer;
            if (peer.parse(item.substr(pos + 1)) != 0) {
                return -1;  // Parsing error
            }
            _peers[key] = peer;
        }
        return 0;
    }

   private:
    std::unordered_map<std::string, braft::PeerId> _peers;
};

}  // namespace mbraft