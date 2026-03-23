/*  Copyright (C) 2022-2024 Damien Gaignon, Daniel Dakhno, José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.devices.flipper.zero;

import androidx.annotation.NonNull;

import java.util.UUID;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.impl.GBDeviceCandidate;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.flipper.zero.support.FlipperZeroSupport;

public class FlipperZeroCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    public boolean supports(GBDeviceCandidate candidate) {
        if (candidate.supportsService(UUID.fromString("00003082-0000-1000-8000-00805f9b34fb"))){
            return true; // TODO need to filter for flipper here
        }
        return false;
    }

    @Override
    public String getManufacturer() {
        return "Flipper Devices";
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return FlipperZeroSupport.class;
    }

    @Override
    public int getBondingStyle() {
        return BONDING_STYLE_NONE;
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_flipper_zero;
    }

    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_flipper;
    }

    @Override
    public DeviceCoordinator.DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceCoordinator.DeviceKind.UNKNOWN;
    }
}
