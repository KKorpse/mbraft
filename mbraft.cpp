#include "src/multi_group_state_machine.h"

int main() {
  mbraft::MulitGroupRaftManager manager;
  mbraft::MulitGroupRaftManagerOptions options;
  manager.init_and_start(options);
  return 0;
}

