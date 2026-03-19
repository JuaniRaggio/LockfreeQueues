#pragma once

#include <array>
#include <atomic>
#include <cstddef>

namespace state {

/**
 * @brief Single-Producer Single-Consumer lock-free ring buffer.
 *
 * One thread may call try_push() and one (different) thread may call
 * try_pop(). Any other access pattern is undefined behaviour.
 *
 * @tparam T         Element type (must be trivially copyable).
 * @tparam Capacity  Maximum number of elements the queue can hold.
 */
template<typename T, std::size_t Capacity>
class SpscQueue {
public:
    /**
     * @brief Push an element into the queue (producer side).
     * @param item  Element to enqueue.
     * @return True if the element was enqueued, false if the queue is full.
     */
    bool try_push(const T& item) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = (tail + 1) % kBufSize;
        if (next == head_.load(std::memory_order_acquire)) {
            return false;
        }
        buffer_[tail] = item;
        tail_.store(next, std::memory_order_release);
        return true;
    }

    /**
     * @brief Pop an element from the queue (consumer side).
     * @param[out] item  Destination populated on success.
     * @return True if an element was dequeued, false if the queue is empty.
     */
    bool try_pop(T& item) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false;
        }
        item = buffer_[head];
        head_.store((head + 1) % kBufSize, std::memory_order_release);
        return true;
    }

    /**
     * @brief Approximate element count (racy but useful for diagnostics).
     * @return Estimated number of elements currently in the queue.
     */
    std::size_t size_approx() const {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        return (tail - head + kBufSize) % kBufSize;
    }

    /**
     * @brief Reset the queue to empty.
     *
     * Only safe to call when no concurrent push/pop is in progress.
     */
    void reset() {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

private:
    static constexpr std::size_t kBufSize = Capacity + 1;
    std::array<T, kBufSize>      buffer_{};
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};

} // namespace state
