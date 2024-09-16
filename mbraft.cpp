#include "src/multi_group_raft_manager.h"

int main() {
  mbraft::MulitGroupRaftManager manager;
  mbraft::MulitGroupRaftManagerOptions options;
  manager.init_and_start(options);
  return 0;
}

