#pragma once

#include "cio/task/task.hpp"
#include "cio/time/instant.hpp"

namespace cio::time {

/**
 * 冻结 current-thread runtime 的 CIO 时钟。
 *
 * 已冻结、runtime 外或不支持暂停的 flavor 会抛出 std::logic_error。
 */
void pause();

/**
 * 恢复已冻结的 CIO 时钟。未冻结或 runtime 外调用会抛出 std::logic_error。
 */
void resume();

/**
 * 把冻结时钟一次性推进 duration，随后主动 yield 一次。
 *
 * 该操作不会等待跨过的所有 timer 对应 task 全部完成；这与 Tokio 1.53.1
 * test-util 的 advance 可观察语义一致。
 */
Task<void> advance(Duration duration);

}  // namespace cio::time
