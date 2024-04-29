#include "multi_group_state_machine.h"

int main() {
  mbraft::MulitGroupRaftManager manager;
  mbraft::MulitGroupRaftManagerOptions options;
  manager.init(options);
  return 0;
}

