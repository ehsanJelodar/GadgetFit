/*  Copyright (C) 2024 Severin von Wnuck-Lipinski

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
package nodomain.freeyourgadget.gadgetfit.service.devices.moondrop;

import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.btbr.TransactionBuilder;
import nodomain.freeyourgadget.gadgetfit.service.serial.AbstractHeadphoneSerialDeviceSupportV2;

public class MoondropSpaceTravelDeviceSupport extends AbstractHeadphoneSerialDeviceSupportV2<MoondropSpaceTravelProtocol> {
    @Override
    protected MoondropSpaceTravelProtocol createDeviceProtocol() {
        return new MoondropSpaceTravelProtocol(getDevice());
    }

    @Override
    protected TransactionBuilder initializeDevice(final TransactionBuilder builder) {
        builder.write(mDeviceProtocol.encodeGetEqualizerPreset());
        builder.write(mDeviceProtocol.encodeGetTouchActions());
        builder.setDeviceState(GBDevice.State.INITIALIZED);

        return builder;
    }
}
