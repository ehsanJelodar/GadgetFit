/*  Copyright (C) 2025 Arjan Schrijver

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
package nodomain.freeyourgadget.gadgetfit.devices.yawell.ring;

import androidx.annotation.NonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class ColmiR12Coordinator extends AbstractYawellRingCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(ColmiR12Coordinator.class);

    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^COLMI R12_.*");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_colmi_r12;
    }

    @Override
    public boolean hasDisplay() {
        return true;
    }

    @Override
    public DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceKind.RING;
    }
}
