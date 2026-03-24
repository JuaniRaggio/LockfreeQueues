#include "../mpsc/mpsc.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// 1. Basic push/pop
// ---------------------------------------------------------------------------
static void test_basic_push_pop() {
    Lockfree::MpscQueue<int, 4> q;
    int val = 0;

    assert(q.try_push(42));
    assert(q.try_pop(val));
    assert(val == 42);

    std::printf("  PASS  basic_push_pop\n");
}

// ---------------------------------------------------------------------------
// 2. FIFO ordering (single producer)
// ---------------------------------------------------------------------------
static void test_fifo_ordering() {
    constexpr std::size_t N = 8;
    Lockfree::MpscQueue<int, N> q;

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
    Lockfree::MpscQueue<int, Cap> q;

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
    Lockfree::MpscQueue<int, 4> q;
    int val = 0;
    assert(!q.try_pop(val));

    std::printf("  PASS  empty_queue\n");
}

// ---------------------------------------------------------------------------
// 5. size_approx
// ---------------------------------------------------------------------------
static void test_size_approx() {
    constexpr std::size_t Cap = 8;
    Lockfree::MpscQueue<int, Cap> q;

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
// 6. Reset
// ---------------------------------------------------------------------------
static void test_reset() {
    constexpr std::size_t Cap = 8;
    Lockfree::MpscQueue<int, Cap> q;

    for (std::size_t i = 0; i < Cap; ++i) {
        q.try_push(static_cast<int>(i));
    }
    assert(q.size_approx() == Cap);

    q.reset();

    assert(q.size_approx() == 0);
    int val;
    assert(!q.try_pop(val));

    // Verify queue is reusable after reset.
    assert(q.try_push(7));
    assert(q.try_pop(val));
    assert(val == 7);

    std::printf("  PASS  reset\n");
}

// ---------------------------------------------------------------------------
// 7. Multi-producer stress -- 4 producers, 1 consumer
// ---------------------------------------------------------------------------
static void test_multi_producer_stress() {
    constexpr std::size_t NumProducers  = 4;
    constexpr std::size_t ItemsEach     = 10'000;
    constexpr std::size_t TotalItems    = NumProducers * ItemsEach;
    constexpr std::size_t Cap           = 1024;

    Lockfree::MpscQueue<std::size_t, Cap> q;

    // Each producer pushes values in range [id*ItemsEach, (id+1)*ItemsEach).
    auto producer = [&](std::size_t id) {
        std::size_t base = id * ItemsEach;
        for (std::size_t i = 0; i < ItemsEach; ++i) {
            while (!q.try_push(base + i)) {
                // spin
            }
        }
    };

    std::vector<std::thread> producers;
    producers.reserve(NumProducers);
    for (std::size_t id = 0; id < NumProducers; ++id) {
        producers.emplace_back(producer, id);
    }

    std::vector<std::size_t> received;
    received.reserve(TotalItems);

    std::thread consumer([&]() {
        while (received.size() < TotalItems) {
            std::size_t val;
            if (q.try_pop(val)) {
                received.push_back(val);
            }
        }
    });

    for (auto& t : producers) {
        t.join();
    }
    consumer.join();

    // Verify all items received exactly once (order is non-deterministic across
    // producers, so we sort and compare).
    assert(received.size() == TotalItems);
    std::sort(received.begin(), received.end());
    for (std::size_t i = 0; i < TotalItems; ++i) {
        assert(received[i] == i);
    }

    std::printf("  PASS  multi_producer_stress\n");
}

// ---------------------------------------------------------------------------
// 8. Burst pattern -- alternating bursts of push and pop
// ---------------------------------------------------------------------------
static void test_burst_pattern() {
    constexpr std::size_t Cap   = 16;
    constexpr std::size_t Burst = 8;
    constexpr std::size_t Rounds = 20;
    Lockfree::MpscQueue<int, Cap> q;

    int next_push = 0;
    int next_pop  = 0;

    for (std::size_t r = 0; r < Rounds; ++r) {
        // Push a burst.
        for (std::size_t i = 0; i < Burst; ++i) {
            assert(q.try_push(next_push));
            ++next_push;
        }
        // Pop a burst.
        for (std::size_t i = 0; i < Burst; ++i) {
            int val = -1;
            assert(q.try_pop(val));
            assert(val == next_pop);
            ++next_pop;
        }
    }

    assert(next_push == next_pop);

    std::printf("  PASS  burst_pattern\n");
}

// ---------------------------------------------------------------------------
// 9. Single-item repeated -- stress free-list recycling
// ---------------------------------------------------------------------------
static void test_single_item_repeated() {
    Lockfree::MpscQueue<int, 1> q;

    for (int i = 0; i < 100'000; ++i) {
        assert(q.try_push(i));
        int val = -1;
        assert(q.try_pop(val));
        assert(val == i);
    }

    std::printf("  PASS  single_item_repeated\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    std::printf("=== MpscQueue tests ===\n");

    test_basic_push_pop();
    test_fifo_ordering();
    test_full_queue();
    test_empty_queue();
    test_size_approx();
    test_reset();
    test_multi_producer_stress();
    test_burst_pattern();
    test_single_item_repeated();

    std::printf("All MpscQueue tests passed.\n");
    return 0;
}
