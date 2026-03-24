#include "../spsc/spsc.h"

#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// 1. Basic push/pop
// ---------------------------------------------------------------------------
static void test_basic_push_pop() {
    Lockfree::SpscQueue<int, 4> q;
    int val = 0;

    assert(q.try_push(42));
    assert(q.try_pop(val));
    assert(val == 42);

    std::printf("  PASS  basic_push_pop\n");
}

// ---------------------------------------------------------------------------
// 2. FIFO ordering
// ---------------------------------------------------------------------------
static void test_fifo_ordering() {
    constexpr std::size_t N = 8;
    Lockfree::SpscQueue<int, N> q;

    for (std::size_t i = 0; i < N; ++i) {
        assert(q.try_push(static_cast<int>(i)));
    }

    for (std::size_t i = 0; i < N; ++i) {
        int val = -1;
        assert(q.try_pop(val));
        assert(val == static_cast<int>(i));
    }

    std::printf("  PASS  fifo_ordering\n");
}

// ---------------------------------------------------------------------------
// 3. Full queue
// ---------------------------------------------------------------------------
static void test_full_queue() {
    constexpr std::size_t Cap = 4;
    Lockfree::SpscQueue<int, Cap> q;

    for (std::size_t i = 0; i < Cap; ++i) {
        assert(q.try_push(static_cast<int>(i)));
    }
    // Queue is full, next push must fail.
    assert(!q.try_push(999));

    std::printf("  PASS  full_queue\n");
}

// ---------------------------------------------------------------------------
// 4. Empty queue
// ---------------------------------------------------------------------------
static void test_empty_queue() {
    Lockfree::SpscQueue<int, 4> q;
    int val = 0;
    assert(!q.try_pop(val));

    std::printf("  PASS  empty_queue\n");
}

// ---------------------------------------------------------------------------
// 5. Wrap-around
// ---------------------------------------------------------------------------
static void test_wrap_around() {
    constexpr std::size_t Cap = 4;
    Lockfree::SpscQueue<int, Cap> q;

    // Fill and drain twice to force the ring indices past the buffer boundary.
    for (int round = 0; round < 3; ++round) {
        for (std::size_t i = 0; i < Cap; ++i) {
            assert(q.try_push(static_cast<int>(round * Cap + i)));
        }
        for (std::size_t i = 0; i < Cap; ++i) {
            int val = -1;
            assert(q.try_pop(val));
            assert(val == static_cast<int>(round * Cap + i));
        }
    }

    std::printf("  PASS  wrap_around\n");
}

// ---------------------------------------------------------------------------
// 6. size_approx
// ---------------------------------------------------------------------------
static void test_size_approx() {
    constexpr std::size_t Cap = 8;
    Lockfree::SpscQueue<int, Cap> q;

    assert(q.size_approx() == 0);

    for (std::size_t i = 0; i < 5; ++i) {
        q.try_push(static_cast<int>(i));
    }
    assert(q.size_approx() == 5);

    int tmp;
    q.try_pop(tmp);
    q.try_pop(tmp);
    assert(q.size_approx() == 3);

    std::printf("  PASS  size_approx\n");
}

// ---------------------------------------------------------------------------
// 7. Reset
// ---------------------------------------------------------------------------
static void test_reset() {
    constexpr std::size_t Cap = 8;
    Lockfree::SpscQueue<int, Cap> q;

    for (std::size_t i = 0; i < Cap; ++i) {
        q.try_push(static_cast<int>(i));
    }
    assert(q.size_approx() == Cap);

    q.reset();

    assert(q.size_approx() == 0);
    int val;
    assert(!q.try_pop(val));

    // Verify queue is usable after reset.
    assert(q.try_push(7));
    assert(q.try_pop(val));
    assert(val == 7);

    std::printf("  PASS  reset\n");
}

// ---------------------------------------------------------------------------
// 8. Concurrent stress -- 1 producer, 1 consumer
// ---------------------------------------------------------------------------
static void test_concurrent_stress() {
    constexpr std::size_t Cap   = 1024;
    constexpr std::size_t Total = 500'000;
    Lockfree::SpscQueue<std::size_t, Cap> q;

    std::thread producer([&]() {
        for (std::size_t i = 0; i < Total; ++i) {
            while (!q.try_push(i)) {
                // spin
            }
        }
    });

    std::vector<std::size_t> received;
    received.reserve(Total);

    std::thread consumer([&]() {
        std::size_t count = 0;
        while (count < Total) {
            std::size_t val;
            if (q.try_pop(val)) {
                received.push_back(val);
                ++count;
            }
        }
    });

    producer.join();
    consumer.join();

    assert(received.size() == Total);
    for (std::size_t i = 0; i < Total; ++i) {
        assert(received[i] == i);
    }

    std::printf("  PASS  concurrent_stress\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    std::printf("=== SpscQueue tests ===\n");

    test_basic_push_pop();
    test_fifo_ordering();
    test_full_queue();
    test_empty_queue();
    test_wrap_around();
    test_size_approx();
    test_reset();
    test_concurrent_stress();

    std::printf("All SpscQueue tests passed.\n");
    return 0;
}
