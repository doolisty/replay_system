#include <cmath>
#include <cstdlib>
#include <iostream>

// Simple test framework (does not depend on Google Test)
#ifndef GTEST_FOUND

#define TEST(suite, name) void suite##_##name()
#define ASSERT_TRUE(x)                                     \
  do {                                                     \
    if (!(x)) {                                            \
      std::cerr << "ASSERT_TRUE failed: " #x << std::endl; \
      std::exit(1);                                        \
    }                                                      \
  } while (0)
#define ASSERT_FALSE(x)                                     \
  do {                                                      \
    if (x) {                                                \
      std::cerr << "ASSERT_FALSE failed: " #x << std::endl; \
      std::exit(1);                                         \
    }                                                       \
  } while (0)
#define ASSERT_EQ(a, b)                                            \
  do {                                                             \
    if ((a) != (b)) {                                              \
      std::cerr << "ASSERT_EQ failed: " #a " != " #b << std::endl; \
      std::exit(1);                                                \
    }                                                              \
  } while (0)
#define ASSERT_NE(a, b)                                            \
  do {                                                             \
    if ((a) == (b)) {                                              \
      std::cerr << "ASSERT_NE failed: " #a " == " #b << std::endl; \
      std::exit(1);                                                \
    }                                                              \
  } while (0)
#define ASSERT_LT(a, b)                                            \
  do {                                                             \
    if (!((a) < (b))) {                                            \
      std::cerr << "ASSERT_LT failed: " #a " >= " #b << std::endl; \
      std::exit(1);                                                \
    }                                                              \
  } while (0)
#define ASSERT_LE(a, b)                                           \
  do {                                                            \
    if (!((a) <= (b))) {                                          \
      std::cerr << "ASSERT_LE failed: " #a " > " #b << std::endl; \
      std::exit(1);                                               \
    }                                                             \
  } while (0)
#define ASSERT_GT(a, b)                                            \
  do {                                                             \
    if (!((a) > (b))) {                                            \
      std::cerr << "ASSERT_GT failed: " #a " <= " #b << std::endl; \
      std::exit(1);                                                \
    }                                                              \
  } while (0)
#define ASSERT_GE(a, b)                                           \
  do {                                                            \
    if (!((a) >= (b))) {                                          \
      std::cerr << "ASSERT_GE failed: " #a " < " #b << std::endl; \
      std::exit(1);                                               \
    }                                                             \
  } while (0)
#define ASSERT_NEAR(a, b, eps)                                     \
  do {                                                             \
    if (std::abs((a) - (b)) > (eps)) {                             \
      std::cerr << "ASSERT_NEAR failed: |" #a " - " #b "| > " #eps \
                << std::endl;                                      \
      std::exit(1);                                                \
    }                                                              \
  } while (0)

#define RUN_TEST(suite, name)                                    \
  do {                                                           \
    std::cout << "Running " #suite "." #name "..." << std::endl; \
    suite##_##name();                                            \
    std::cout << "  PASSED" << std::endl;                        \
  } while (0)

#else

#include <gtest/gtest.h>

#endif

// Test entry points are defined in each test file
