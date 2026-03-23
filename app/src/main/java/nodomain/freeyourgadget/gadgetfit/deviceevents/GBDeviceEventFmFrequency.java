/*  Copyright (C) 2018-2025 José Rebelo

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

import androidx.annotation.NonNull;

import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class GBDeviceEventFmFrequency extends GBDeviceEvent {
    public final float frequency;

    public GBDeviceEventFmFrequency(final float frequency) {
        this.frequency = frequency;
    }

    @NonNull
    @Override
    public String toString() {
        return super.toString() + "frequency: " + frequency;
    }

    @Override
    public void evaluate(final Context context, final GBDevice device) {
        device.setExtraInfo("fm_frequency", frequency);
        device.sendDeviceUpdateIntent(context);
    }
}
