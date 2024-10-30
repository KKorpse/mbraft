#include <gflags/gflags.h>  // DEFINE_*
#include <openssl/ec.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "src/multi_group_raft_manager.h"

DEFINE_int32(num_nodes, 1, "Max election delay time allowed by user");

DEFINE_int32(num_streams, 1, "Max election delay time allowed by user");

void election_rtt_test() {
    std::vector<std::unique_ptr<mbraft::MulitGroupRaftManager>> managers;
    for (int i = 0; i < FLAGS_num_nodes; i++) {
        managers.emplace_back(new mbraft::MulitGroupRaftManager);
        mbraft::MulitGroupRaftManagerOptions options;
        options.group_count = FLAGS_num_streams;
        options.confg_file_path = "configs/" + std::to_string(FLAGS_num_nodes) +
                                  "node" + std::to_string(FLAGS_num_streams) +
                                  "stream" + std::to_string(i) + ".json";
        managers[i]->init_and_start(options);
    }

    while (true) {
        sleep(1);
    }
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    election_rtt_test();
    return 0;
}
