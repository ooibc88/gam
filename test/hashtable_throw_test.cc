// Copyright (c) 2018 The GAM Authors 

#include <iostream>
#include <exception>
#include <string>

#include "hashtable.h"

int main() {
  HashTable<int, std::string> hashtable;
  hashtable[0] = "string_0";
  try {
    std::string x = hashtable[0];
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  try {
    std::string x = hashtable.at(0);
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  try {
    std::string x = hashtable[1];
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  try {
    std::string x = hashtable.at(1);
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  hashtable[1] = "string_1";
  try {
    std::string x = hashtable[1];
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  try {
    std::string x = hashtable.at(1);
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  hashtable.at(1) = "string_1_1";
  try {
    std::string x = hashtable[1];
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  try {
    std::string x = hashtable.at(1);
    std::cout << x << std::endl;
  } catch (const exception& e) {
    std::cout << e.what() << std::endl;
  }
  return EXIT_SUCCESS;
}
