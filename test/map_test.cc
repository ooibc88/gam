// Copyright (c) 2018 The GAM Authors 


#include <list>
#include <cassert>
#include "settings.h"
#include "map.h"

int main() {
  Map<int, int, 10> m { "m" };
  int iter = 1;
  for (int i = 0; i < iter; i++) {
    m[i] = i - 1;
  }
  for (int i = 0; i < iter; i++) {
    assert(m[i] == i - 1);
  }
  printf("int check succeeded!!\n");

  Map<std::string, std::string> sm("sm");
  for (int i = 0; i < iter; i++) {
    sm[std::to_string(i)] = std::to_string(i + 1);
  }
  for (int i = 0; i < iter; i++) {
    assert(sm[std::to_string(i)] == std::to_string(i + 1));
  }
  printf("string check succeeded!!\n");

  try {
    sm.at(std::to_string(iter));
  } catch (std::exception& e) {
    printf("key %d not exists (err = %s)\n", iter, e.what());
  }

  size_t hv = m.lock(1);
  m[1] = 100;
  m.unlock_hv(hv);

  m.lock(1);
  assert(m[1] == 100);
  m.unlock(1);
  return 0;
}

