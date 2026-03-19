#pragma once
/**
 * @file thread_safe_queue.h
 * @brief Lock-free SPSC and MPSC ring-buffer queues.
 *
 * These queues rely solely on std::atomic with explicit memory ordering --
 * no mutex, no condition_variable. Suitable for real-time threads where
 * blocking is unacceptable.
 */

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

/**
 * @brief Multiple-Producer Single-Consumer lock-free ring buffer.
 *
 * Producers reserve a slot via CAS on the tail index, write data, and
 * mark the slot as ready. The single consumer only advances when the
 * head slot is marked ready. Contention is negligible at the expected
 * push rates (~32 pushes/s from Health + Control threads).
 *
 * @tparam T         Element type (must be trivially copyable).
 * @tparam Capacity  Maximum number of elements the queue can hold.
 */
template<typename T, std::size_t Capacity>
class MpscQueue {
public:
    /**
     * @brief Push an element into the queue (any producer thread).
     * @param item  Element to enqueue.
     * @return True if the element was enqueued, false if the queue is full.
     */
    bool try_push(const T& item) {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        std::size_t next;
        do {
            next = (tail + 1) % kBufSize;
            if (next == head_.load(std::memory_order_acquire)) {
                return false;
            }
        } while (!tail_.compare_exchange_weak(tail, next, std::memory_order_acq_rel,
                                              std::memory_order_relaxed));

        buffer_[tail] = item;
        written_[tail].store(true, std::memory_order_release);
        return true;
    }

    /**
     * @brief Pop an element from the queue (single consumer thread only).
     * @param[out] item  Destination populated on success.
     * @return True if an element was dequeued, false if the queue is empty
     *         or the head slot has not been fully written yet.
     */
    bool try_pop(T& item) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false;
        }
        if (!written_[head].load(std::memory_order_acquire)) {
            return false;
        }
        item = buffer_[head];
        written_[head].store(false, std::memory_order_release);
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
     * @WARN! Only safe to call when no concurrent push/pop is in progress.
     */
    void reset() {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
        for (std::size_t i = 0; i < kBufSize; ++i) {
            written_[i].store(false, std::memory_order_relaxed);
        }
    }

private:
    static constexpr std::size_t            kBufSize = Capacity + 1;
    std::array<T, kBufSize>                 buffer_{};
    std::array<std::atomic<bool>, kBufSize> written_{};
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};

} // namespace state
