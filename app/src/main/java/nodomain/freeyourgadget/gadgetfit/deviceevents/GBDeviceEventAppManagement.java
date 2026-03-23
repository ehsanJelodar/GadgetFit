/*  Copyright (C) 2015-2024 Andreas Shimokawa

    This file is part of gadgetbridge.

    gadgetbridge is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    gadgetbridge is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>. */
package nodomain.freeyourgadget.gadgetfit.deviceevents;

import android.content.Context;

import java.util.UUID;

import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class GBDeviceEventAppManagement extends GBDeviceEvent {
    public Event event = Event.UNKNOWN;
    public EventType type = EventType.UNKNOWN;
    public int token = -1;
    public UUID uuid = null;

    @Override
    public void evaluate(final Context context, final GBDevice device) {
        // FIXME: Pebble-specific, handled in support class
    }

    public enum EventType {
        UNKNOWN,
        INSTALL,
        DELETE,
        START,
        STOP,
    }

    public enum Event {
        UNKNOWN,
        SUCCESS,
        ACKNOWLEDGE,
        FAILURE,
        REQUEST,
    }
}
