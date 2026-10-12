// Copyright (C) 2023-2025  Lightpanda (Selecy SAS)
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

// https://w3c.github.io/requestidlecallback/#the-idledeadline-interface
const lp = @import("lightpanda");

const Scheduler = @import("../js/Scheduler.zig");

const IdleDeadline = @This();

// boot-clock ms by which the callback should yield
_deadline: u64,
_did_timeout: bool,

pub fn init(period: Scheduler.IdlePeriod) IdleDeadline {
    return .{ ._deadline = period.deadline, ._did_timeout = period.did_timeout };
}

fn timeRemaining(self: *const IdleDeadline) f64 {
    const now = lp.datetime.milliTimestamp(.boot);
    if (now >= self._deadline) {
        return 0;
    }
    return @floatFromInt(self._deadline - now);
}

fn getDidTimeout(self: *const IdleDeadline) bool {
    return self._did_timeout;
}

pub const JsApi = struct {
    const js = @import("../js/js.zig");
    pub const bridge = js.Bridge(IdleDeadline);

    pub const Meta = struct {
        pub const name = "IdleDeadline";
        pub const prototype_chain = bridge.prototypeChain();
        pub var class_id: bridge.ClassId = undefined;
    };

    pub const timeRemaining = bridge.function(IdleDeadline.timeRemaining, .{});
    pub const didTimeout = bridge.accessor(IdleDeadline.getDidTimeout, null, .{});
};
