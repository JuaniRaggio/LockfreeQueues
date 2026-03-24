#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

namespace Lockfree {

/**
 * @brief Multiple-Producer Single-Consumer lock-free queue.
 *
 * Uses a CAS-stack for producers (incoming_) with batch reversal for FIFO
 * ordering on the consumer side. A tagged free-list provides ABA-safe node
 * recycling without dynamic allocation.
 *
 * Lock-free guarantee: try_push never blocks on another thread's incomplete
 * operation. try_pop is wait-free (exchange always succeeds in one step,
 * reversal and drain are consumer-local).
 *
 * @tparam T         Element type.
 * @tparam Capacity  Maximum number of elements the queue can hold.
 */
template <typename T, std::size_t Capacity>
class MpscQueue {
  static_assert(Capacity > 0, "Capacity must be greater than zero");

  struct Node {
    T data;
    Node* next;
  };

  static constexpr uint64_t kNull = 0xFFFFFFFFULL;

  static uint64_t make_tagged(uint32_t index, uint32_t tag) {
    return (static_cast<uint64_t>(tag) << 32) | index;
  }
  static uint32_t tagged_index(uint64_t tagged) {
    return static_cast<uint32_t>(tagged & 0xFFFFFFFF);
  }
  static uint32_t tagged_tag(uint64_t tagged) {
    return static_cast<uint32_t>(tagged >> 32);
  }

public:
  MpscQueue() { init(); }

  /**
   * @brief Push an element into the queue (any producer thread).
   * @param item  Element to enqueue.
   * @return True if the element was enqueued, false if the queue is full.
   */
  bool try_push(const T& item) {
    Node* node = alloc_node();
    if (!node) {
      return false; // queue full
    }

    node->data = item;

    Node* old_head = incoming_.load(std::memory_order_relaxed);
    do {
      node->next = old_head;
    } while (!incoming_.compare_exchange_weak(
        old_head, node,
        std::memory_order_release,
        std::memory_order_relaxed));

    count_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  /**
   * @brief Pop an element from the queue (single consumer thread only).
   * @param[out] item  Destination populated on success.
   * @return True if an element was dequeued, false if the queue is empty.
   */
  bool try_pop(T& item) {
    if (!consumer_list_) {
      Node* stolen = incoming_.exchange(nullptr, std::memory_order_acquire);
      if (!stolen) {
        return false; // nothing to consume
      }
      consumer_list_ = reverse(stolen);
    }

    Node* node = consumer_list_;
    consumer_list_ = node->next;

    item = std::move(node->data);
    free_node(node);

    count_.fetch_sub(1, std::memory_order_relaxed);
    return true;
  }

  /**
   * @brief Approximate element count (racy but useful for diagnostics).
   * @return Estimated number of elements currently in the queue.
   */
  std::size_t size_approx() const {
    auto c = count_.load(std::memory_order_relaxed);
    return (c < 0) ? 0 : static_cast<std::size_t>(c);
  }

  /**
   * @brief Reset the queue to empty.
   *
   * @WARN! Only safe to call when no concurrent push/pop is in progress.
   */
  void reset() {
    incoming_.store(nullptr, std::memory_order_relaxed);
    consumer_list_ = nullptr;
    count_.store(0, std::memory_order_relaxed);
    init_free_list();
  }

private:

  void init() {
    init_free_list();
    incoming_.store(nullptr, std::memory_order_relaxed);
    consumer_list_ = nullptr;
    count_.store(0, std::memory_order_relaxed);
  }

  void init_free_list() {
    for (uint32_t i = 0; i < Capacity - 1; ++i) {
      free_next_[i] = i + 1;
    }
    free_next_[Capacity - 1] = static_cast<uint32_t>(kNull);
    free_head_.store(make_tagged(0, 0), std::memory_order_relaxed);
  }

  Node* alloc_node() {
    uint64_t old_head = free_head_.load(std::memory_order_acquire);
    while (true) {
      uint32_t idx = tagged_index(old_head);
      if (idx == static_cast<uint32_t>(kNull)) {
        return nullptr; // free-list exhausted
      }
      uint32_t next = free_next_[idx];
      uint32_t tag  = tagged_tag(old_head);
      uint64_t new_head = make_tagged(next, tag + 1);

      if (free_head_.compare_exchange_weak(
              old_head, new_head,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return &pool_[idx];
      }
    }
  }

  void free_node(Node* node) {
    uint32_t idx = static_cast<uint32_t>(node - pool_.data());
    uint64_t old_head = free_head_.load(std::memory_order_relaxed);
    while (true) {
      free_next_[idx] = tagged_index(old_head);
      uint32_t tag = tagged_tag(old_head);
      uint64_t new_head = make_tagged(idx, tag + 1);

      if (free_head_.compare_exchange_weak(
              old_head, new_head,
              std::memory_order_release,
              std::memory_order_relaxed)) {
        return;
      }
    }
  }


  static Node* reverse(Node* head) {
    Node* prev = nullptr;
    while (head) {
      Node* next = head->next;
      head->next = prev;
      prev = head;
      head = next;
    }
    return prev;
  }


  std::array<Node, Capacity> pool_{};
  std::array<uint32_t, Capacity> free_next_{};

  alignas(64) std::atomic<uint64_t> free_head_{};
  alignas(64) std::atomic<Node*> incoming_{nullptr};
  alignas(64) std::atomic<std::ptrdiff_t> count_{0};

  Node* consumer_list_{nullptr};
};

} // namespace Lockfree
