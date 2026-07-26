#include "cio/runtime/runtime.hpp"

namespace cio::runtime {

// 保留一个编译单元，使不同平台后端可以在后续切片中隔离实现而不改变公开头文件。
static_assert(__cplusplus >= 202002L, "CIO 只支持 C++20 或更新的语言模式");

}  // namespace cio::runtime
