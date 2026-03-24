# LockfreeQueues

Header-only C++17 lock-free queue library. No external dependencies.

## SpscQueue

**Single-Producer Single-Consumer** queue built on a fixed-size ring buffer.

Both `try_push` (producer) and `try_pop` (consumer) are **wait-free** -- each
completes in a bounded number of steps with no CAS loops. The implementation
uses a one-slot-wasted strategy (`Capacity + 1` internal slots) so that head
and tail can be compared without an extra counter.

Constraints: exactly one thread may call `try_push` and exactly one (different)
thread may call `try_pop`. Violating this is undefined behaviour.

## MpscQueue

**Multiple-Producer Single-Consumer** queue using a CAS-based push stack with
batch reversal for FIFO ordering on the consumer side.

Producers push nodes onto an atomic LIFO stack (`incoming_`) via
`compare_exchange_weak` -- this is **lock-free** (producers never block on each
other, though they may retry a CAS). The single consumer atomically steals the
entire stack with `exchange`, reverses it into a local FIFO list, and drains it
-- making `try_pop` **wait-free**.

A tagged free-list (index + monotonic tag packed into a 64-bit word) provides
ABA-safe node recycling without any dynamic allocation.

