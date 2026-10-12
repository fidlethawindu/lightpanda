// Copyright (C) 2023-2026  Lightpanda (Selecy SAS)
//
// Francis Bouvier <francis@lightpanda.io>
// Pierre Tachoire <pierre@lightpanda.io>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

const std = @import("std");
const lp = @import("lightpanda");

const log = lp.log;
const Allocator = std.mem.Allocator;

// low bits of a Task.key hold the FIFO sequence; the high 40 hold run_at
// (~34 years of process uptime).
const SEQ_BITS = 24;
const SEQ_MASK = (@as(u64, 1) << SEQ_BITS) - 1;

const Scheduler = @This();

_sequence: u64,
allocator: Allocator,

// Tasks queued to be run at a specific time
timed: Queue,

// Tasks queued to be run at the next (optimization for when .run_at = 0)
immediate: Immediate,

// Tasks queued to run before anything else (i.e. scheduler.yield)
front: Immediate,

// Some things (e.g. IndexedDB) can have operations that are only valid for a
// specific task boundary. So every time we start a task, we increment the
// scheduler's generation. Code can snapshot this version and then compare it
// later to see if we're still in the same task.
generation: u64,

// Number of pending tasks with blocks_done=true. While > 0, the page isn't
// considered done.
blocking: u32,

// Idle callbacks (requestIdleCallback), in the order they were requested.
// https://w3c.github.io/requestidlecallback/
idle: std.ArrayList(IdleTask),

// No idle period starts before this boot-clock ms: the deadline of the last
// one. A callback requested during an idle period waits for the next one, so a
// callback that requests itself again runs once per period, not in a busy loop.
next_idle_period_at: u64,

// While an idle callback runs: the deadline and whether it was its timeout
// that ran it, for the IdleDeadline it receives.
idle_period: ?IdlePeriod,

pub fn init(allocator: Allocator) Scheduler {
    return .{
        ._sequence = 0,
        .allocator = allocator,
        .generation = 0,
        .blocking = 0,
        .idle = .empty,
        .next_idle_period_at = 0,
        .idle_period = null,
        .timed = Queue.initContext({}),
        .immediate = .{},
        .front = .{},
    };
}

pub fn deinit(self: *Scheduler) void {
    self.finalizeTasks();
}

pub fn reset(self: *Scheduler) void {
    self.finalizeTasks();
    self.blocking = 0;
    self.idle.clearRetainingCapacity();
    self.next_idle_period_at = 0;
    self.idle_period = null;
    self.front.clear();
    self.immediate.clear();
    self.timed.clearRetainingCapacity();
}

pub const AddOpts = struct {
    name: []const u8 = "",
    front: bool = false, // run before any other tasks, multi-fronts are FIFO amongst themselves
    blocks_done: bool = true,
    finalizer: ?Finalizer = null,
};
pub fn add(self: *Scheduler, ctx: *anyopaque, cb: Callback, run_in_ms: u32, opts: AddOpts) !void {
    if (comptime lp.IS_DEBUG) {
        log.debug(.scheduler, "scheduler.add", .{ .name = opts.name, .run_in_ms = run_in_ms, .blocks_done = opts.blocks_done });
    }
    if (comptime lp.IS_DEBUG) {
        // run_in_ms is ignored when opts.front == true, but let's enforce this
        // so that a caller isn't expecting something we don't support
        std.debug.assert(opts.front == false or run_in_ms == 0);
    }

    const run_at = lp.datetime.milliTimestamp(.boot) + run_in_ms;
    const task: Task = .{
        .ctx = ctx,
        .callback = cb,
        .name = opts.name,
        .key = self.nextKey(run_at),
        .finalizer = opts.finalizer,
        .blocks_done = opts.blocks_done,
    };

    if (opts.front) {
        try self.front.push(self.allocator, task);
    } else if (run_in_ms == 0) {
        try self.immediate.push(self.allocator, task);
    } else {
        try self.timed.push(self.allocator, task);
    }

    if (opts.blocks_done) {
        self.blocking += 1;
    }
}

// The longest an idle period lasts, per the spec's recommendation: long enough
// for useful work, short enough to stay responsive to new input.
const IDLE_PERIOD_MAX_MS = 50;

pub const IdlePeriod = struct {
    // boot-clock ms by which the callback should yield
    deadline: u64,
    did_timeout: bool,
};

const IdleTask = struct {
    task: Task,
    // boot-clock ms past which the callback runs whether or not the page is
    // idle (requestIdleCallback's timeout option)
    timeout_at: ?u64,
};

// Queues `cb` to run in the next idle period: once no task is ready to run.
// With `timeout_ms`, it runs at that point at the latest, idle or not.
pub fn addIdle(self: *Scheduler, ctx: *anyopaque, cb: Callback, timeout_ms: ?u32, opts: AddOpts) !void {
    if (comptime lp.IS_DEBUG) {
        log.debug(.scheduler, "scheduler.addIdle", .{ .name = opts.name, .timeout_ms = timeout_ms, .blocks_done = opts.blocks_done });
        std.debug.assert(opts.front == false);
    }

    const now = lp.datetime.milliTimestamp(.boot);
    try self.idle.append(self.allocator, .{
        .task = .{
            .ctx = ctx,
            .callback = cb,
            .name = opts.name,
            .key = self.nextKey(now),
            .finalizer = opts.finalizer,
            .blocks_done = opts.blocks_done,
        },
        .timeout_at = if (timeout_ms) |ms| now + ms else null,
    });

    if (opts.blocks_done) {
        self.blocking += 1;
    }
}

pub fn run(self: *Scheduler) !void {
    const start = lp.datetime.milliTimestamp(.boot);
    var now = start;

    while (self.popReady(now)) |task_| {
        var task = task_;
        if (comptime lp.IS_DEBUG) {
            log.debug(.scheduler, "scheduler.runTask", .{ .name = task.name });
        }

        self.generation +%= 1;

        const repeat_in_ms = task.callback(task.ctx) catch |err| {
            log.debug(.scheduler, "task.callback", .{ .name = task.name, .err = err });
            continue;
        };

        if (repeat_in_ms) |ms| {
            // Task cannot be repeated immediately, and they should know that
            if (comptime lp.IS_DEBUG) {
                std.debug.assert(ms != 0);
            }
            // A task that endlessly reschedules itself would keep the page
            // alive forever, so repeats never block completion.
            task.blocks_done = false;
            task.key = self.nextKey(now + ms);
            try self.timed.push(self.allocator, task);
        }

        now = lp.datetime.milliTimestamp(.boot);
        if (now - start > 500) {
            return;
        }
    }

    try self.runIdle();
}

// Idle callbacks whose timeout has passed run first, as tasks of their own.
// Then, when no task is ready, an idle period starts and runs the callbacks
// requested before it, until its deadline.
fn runIdle(self: *Scheduler) !void {
    var now = lp.datetime.milliTimestamp(.boot);

    var i: usize = 0;
    while (i < self.idle.items.len) {
        const timeout_at = self.idle.items[i].timeout_at orelse {
            i += 1;
            continue;
        };
        if (timeout_at > now) {
            i += 1;
            continue;
        }
        const idle_task = self.idle.orderedRemove(i);
        self.runIdleTask(idle_task.task, .{ .deadline = now, .did_timeout = true });
        now = lp.datetime.milliTimestamp(.boot);
    }

    if (self.idle.items.len == 0 or now < self.next_idle_period_at) {
        return;
    }
    if (self.taskReady(now)) {
        return;
    }

    var deadline = now + IDLE_PERIOD_MAX_MS;
    if (self.timed.peek()) |next| {
        deadline = @min(deadline, next.runAt());
    }
    self.next_idle_period_at = deadline;

    // Callbacks requested while this period runs wait for the next one.
    var remaining = self.idle.items.len;
    while (remaining > 0 and now < deadline) : (remaining -= 1) {
        const idle_task = self.idle.orderedRemove(0);
        self.runIdleTask(idle_task.task, .{ .deadline = deadline, .did_timeout = false });
        now = lp.datetime.milliTimestamp(.boot);
    }
}

fn runIdleTask(self: *Scheduler, task: Task, period: IdlePeriod) void {
    if (task.blocks_done) {
        self.blocking -= 1;
    }
    self.generation +%= 1;
    self.idle_period = period;
    defer self.idle_period = null;

    const repeat_in_ms = task.callback(task.ctx) catch |err| {
        log.debug(.scheduler, "idle task.callback", .{ .name = task.name, .err = err });
        return;
    };
    if (comptime lp.IS_DEBUG) {
        // an idle callback asks for the next idle period itself
        std.debug.assert(repeat_in_ms == null);
    }
}

// Whether a task other than an idle callback is ready to run.
fn taskReady(self: *Scheduler, now: u64) bool {
    if (self.immediate.peek() != null or self.front.peek() != null) {
        return true;
    }
    const task = self.timed.peek() orelse return false;
    return task.runAt() <= now;
}

pub fn hasReadyTasks(self: *Scheduler) bool {
    const ms = self.msToNext() orelse return false;
    return ms == 0;
}

pub fn blocksCompletion(self: *const Scheduler) bool {
    return self.blocking > 0;
}

pub fn msToNext(self: *Scheduler) ?u64 {
    if (self.immediate.peek() != null or self.front.peek() != null) {
        return 0;
    }
    const now = lp.datetime.milliTimestamp(.boot);
    var next: ?u64 = null;
    if (self.timed.peek()) |task| {
        next = task.runAt();
    }
    if (self.idle.items.len > 0) {
        // the next idle period, or an earlier timeout
        var idle_at = self.next_idle_period_at;
        for (self.idle.items) |idle_task| {
            if (idle_task.timeout_at) |timeout_at| {
                idle_at = @min(idle_at, timeout_at);
            }
        }
        next = if (next) |n| @min(n, idle_at) else idle_at;
    }
    const run_at = next orelse return null;
    return if (run_at <= now) 0 else run_at - now;
}

fn nextKey(self: *Scheduler, run_at: u64) u64 {
    if (comptime lp.IS_DEBUG) {
        std.debug.assert(run_at >> (64 - SEQ_BITS) == 0);
    }
    self._sequence +%= 1;
    return (run_at << SEQ_BITS) | (self._sequence & SEQ_MASK);
}

fn popReady(self: *Scheduler, now: u64) ?Task {
    const task = task: {
        if (self.front.peek() != null) {
            break :task self.front.pop();
        }

        const immediate = self.immediate.peek();
        const timed = self.timed.peek();

        const pick_immediate = if (immediate == null)
            false
        else if (timed == null)
            true
        else
            immediate.?.key < timed.?.key;

        if (pick_immediate) {
            break :task self.immediate.pop();
        }

        const candidate = timed orelse return null;
        if (candidate.runAt() > now) {
            return null;
        }

        break :task self.timed.pop().?;
    };

    if (task.blocks_done) {
        self.blocking -= 1;
    }
    return task;
}

fn finalizeTasks(self: *Scheduler) void {
    for (self.front.pending()) |task| {
        if (task.finalizer) |func| {
            func(task.ctx);
        }
    }
    for (self.immediate.pending()) |task| {
        if (task.finalizer) |func| {
            func(task.ctx);
        }
    }
    var it = self.timed.iterator();
    while (it.next()) |t| {
        if (t.finalizer) |func| {
            func(t.ctx);
        }
    }
    for (self.idle.items) |idle_task| {
        if (idle_task.task.finalizer) |func| {
            func(idle_task.task.ctx);
        }
    }
}

const Task = struct {
    key: u64, // (run_at << SEQ_BITS) | sequence — tasks execute in key order
    ctx: *anyopaque,
    name: []const u8,
    blocks_done: bool,
    callback: Callback,
    finalizer: ?Finalizer,

    fn runAt(self: Task) u64 {
        return self.key >> SEQ_BITS;
    }
};

const Queue = std.PriorityQueue(Task, void, struct {
    fn compare(_: void, a: Task, b: Task) std.math.Order {
        return std.math.order(a.key, b.key);
    }
}.compare);

// FIFO for 0-delay and front tasks. They dominate real pages and this avoids
// the PriorityQueue sorting.
const Immediate = struct {
    head: usize = 0,
    tasks: std.ArrayList(Task) = .empty,

    fn push(self: *Immediate, allocator: Allocator, task: Task) !void {
        // pop can only reset tasks when its fully drained (which might not
        // be possible to do per-run in our 500ms budget). To prevent timer loop
        // from preventing the cleanup from ever happening, we compact on push
        // once we have enough entries to justify it (16 is arbitrarily chosen).
        const head = self.head;
        const live = self.tasks.items.len - head;
        if (head > 16 and head >= live) {
            std.mem.copyForwards(Task, self.tasks.items[0..live], self.tasks.items[head..]);
            self.tasks.shrinkRetainingCapacity(live);
            self.head = 0;
        }
        return self.tasks.append(allocator, task);
    }

    fn peek(self: *const Immediate) ?Task {
        if (self.head == self.tasks.items.len) {
            return null;
        }
        return self.tasks.items[self.head];
    }

    fn pop(self: *Immediate) Task {
        const task = self.tasks.items[self.head];
        self.head += 1;
        if (self.head == self.tasks.items.len) {
            self.clear();
        }
        return task;
    }

    fn pending(self: *const Immediate) []Task {
        return self.tasks.items[self.head..];
    }

    fn clear(self: *Immediate) void {
        self.head = 0;
        self.tasks.clearRetainingCapacity();
    }
};

const Callback = *const fn (ctx: *anyopaque) anyerror!?u32;
const Finalizer = *const fn (ctx: *anyopaque) void;

const testing = @import("../../testing.zig");
test "Scheduler: immediates are FIFO, front runs first" {
    var s = Scheduler.init(testing.arena_allocator);
    var ran: std.ArrayList(u8) = .empty;
    var t1: TestTask = .{ .id = 1, .ran = &ran };
    var t2: TestTask = .{ .id = 2, .ran = &ran };
    var t3: TestTask = .{ .id = 3, .ran = &ran };
    var t4: TestTask = .{ .id = 4, .ran = &ran };

    try s.add(&t1, TestTask.run, 0, .{});
    try s.add(&t2, TestTask.run, 0, .{});
    try s.add(&t3, TestTask.run, 0, .{ .front = true });
    try s.add(&t4, TestTask.run, 0, .{ .front = true });
    try s.run();
    try testing.expectEqualSlices(u8, &.{ 3, 4, 1, 2 }, ran.items);
}

test "Scheduler: due timed tasks run before newer immediates" {
    var s = Scheduler.init(testing.arena_allocator);
    var ran: std.ArrayList(u8) = .empty;
    var t1: TestTask = .{ .id = 1, .ran = &ran };
    var t2: TestTask = .{ .id = 2, .ran = &ran };

    try s.add(&t1, TestTask.run, 1, .{});
    lp.io.sleep(.fromMilliseconds(5), .awake) catch {};
    try s.add(&t2, TestTask.run, 0, .{});
    try s.run();
    try testing.expectEqualSlices(u8, &.{ 1, 2 }, ran.items);
}

test "Scheduler: blocksCompletion" {
    var s = Scheduler.init(testing.arena_allocator);
    var ran: std.ArrayList(u8) = .empty;
    var t1: TestTask = .{ .id = 1, .ran = &ran, .repeat = 20 };
    var t2: TestTask = .{ .id = 2, .ran = &ran };

    try testing.expectEqual(false, s.blocksCompletion());
    try s.add(&t1, TestTask.run, 0, .{});
    try s.add(&t2, TestTask.run, 0, .{ .blocks_done = false });
    try testing.expectEqual(true, s.blocksCompletion());

    try s.run();
    try testing.expectEqualSlices(u8, &.{ 1, 2 }, ran.items);
    // t1 was re-queued 20ms out, but repeats don't block completion
    try testing.expectEqual(1, s.timed.count());
    try testing.expectEqual(false, s.blocksCompletion());
}

test "Scheduler: idle tasks run once no task is ready, and block completion" {
    var s = Scheduler.init(testing.arena_allocator);
    var ran: std.ArrayList(u8) = .empty;
    var t1: TestTask = .{ .id = 1, .ran = &ran, .idle_of = &s };
    var t2: TestTask = .{ .id = 2, .ran = &ran };
    var t3: TestTask = .{ .id = 3, .ran = &ran };

    try s.addIdle(&t1, TestTask.run, 10_000, .{});
    try s.add(&t2, TestTask.run, 0, .{});
    try s.add(&t3, TestTask.run, 10_000, .{ .blocks_done = false });
    try testing.expectEqual(true, s.blocksCompletion());
    try testing.expectEqual(0, s.msToNext().?);

    try s.run();
    // the immediate first, then the idle period, long before the timeout
    try testing.expectEqualSlices(u8, &.{ 2, 1 }, ran.items);
    try testing.expectEqual(false, t1.idle_period.?.did_timeout);
    try testing.expectEqual(null, s.idle_period);
    try testing.expectEqual(false, s.blocksCompletion());
    try testing.expectEqual(0, s.idle.items.len);
}

test "Scheduler: an idle task past its timeout runs as timed out" {
    var s = Scheduler.init(testing.arena_allocator);
    var ran: std.ArrayList(u8) = .empty;
    var t1: TestTask = .{ .id = 1, .ran = &ran, .idle_of = &s };
    var t2: TestTask = .{ .id = 2, .ran = &ran, .repeat = 1 };

    try s.addIdle(&t1, TestTask.run, 1, .{});
    lp.io.sleep(.fromMilliseconds(5), .awake) catch {};
    try s.add(&t2, TestTask.run, 0, .{});
    try s.run();
    try testing.expectEqualSlices(u8, &.{ 2, 1 }, ran.items);
    try testing.expectEqual(true, t1.idle_period.?.did_timeout);
    try testing.expectEqual(0, s.idle.items.len);
}

test "Scheduler: an idle task requested during an idle period waits for the next" {
    var s = Scheduler.init(testing.arena_allocator);
    var ran: std.ArrayList(u8) = .empty;
    var t2: TestTask = .{ .id = 2, .ran = &ran };
    var t1: TestTask = .{ .id = 1, .ran = &ran, .then_idle = .{ .scheduler = &s, .task = &t2 } };

    try s.addIdle(&t1, TestTask.run, null, .{});
    try s.run();
    try testing.expectEqualSlices(u8, &.{1}, ran.items);
    try testing.expectEqual(1, s.idle.items.len);
    // the next idle period starts at this one's deadline
    try testing.expect(s.msToNext().? > 0);
    try testing.expect(s.msToNext().? <= IDLE_PERIOD_MAX_MS);
}

test "Scheduler: immediate queue reclaims consumed prefix" {
    var im: Immediate = .{};
    const dummy: Task = .{
        .key = 0,
        .name = "",
        .ctx = undefined,
        .finalizer = null,
        .blocks_done = false,
        .callback = undefined,
    };

    // keep 2 tasks live at all times so pop never fully drains
    try im.push(testing.arena_allocator, dummy);
    try im.push(testing.arena_allocator, dummy);
    for (0..10_000) |_| {
        _ = im.pop();
        try im.push(testing.arena_allocator, dummy);
    }
    try testing.expect(im.tasks.items.len <= 64);
}

const TestTask = struct {
    id: u8,
    ran: *std.ArrayList(u8),
    repeat: ?u32 = null,
    // idle tasks: the scheduler, to record the idle period they ran in
    idle_of: ?*Scheduler = null,
    idle_period: ?IdlePeriod = null,
    then_idle: ?struct { scheduler: *Scheduler, task: *TestTask } = null,

    fn run(ptr: *anyopaque) anyerror!?u32 {
        const self: *TestTask = @ptrCast(@alignCast(ptr));
        if (self.idle_of) |scheduler| {
            self.idle_period = scheduler.idle_period;
        }
        if (self.then_idle) |then| {
            try then.scheduler.addIdle(then.task, TestTask.run, null, .{});
        }
        try self.ran.append(testing.arena_allocator, self.id);
        defer self.repeat = null;
        return self.repeat;
    }
};
