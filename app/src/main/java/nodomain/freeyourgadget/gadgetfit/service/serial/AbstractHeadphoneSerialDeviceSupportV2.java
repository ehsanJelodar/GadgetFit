/*  Copyright (C) 2025 José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.service.serial;

import android.bluetooth.BluetoothAdapter;
import android.content.Context;

import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.CallSpec;
import nodomain.freeyourgadget.gadgetfit.model.NotificationSpec;
import nodomain.freeyourgadget.gadgetfit.service.AbstractHeadphoneBTBRDeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.HeadphoneHelper;

/**
 * @deprecated Use {@link AbstractHeadphoneBTBRDeviceSupport}
 */
@Deprecated
public abstract class AbstractHeadphoneSerialDeviceSupportV2<T extends GBDeviceProtocol>
        extends AbstractSerialDeviceSupportV2<T> implements HeadphoneHelper.Callback {
    private HeadphoneHelper headphoneHelper;

    @Override
    public void onSetCallState(final CallSpec callSpec) {
        headphoneHelper.onSetCallState(callSpec);
    }

    @Override
    public void onNotification(final NotificationSpec notificationSpec) {
        headphoneHelper.onNotification(notificationSpec);
    }

    @Override
    public void setContext(final GBDevice gbDevice, final BluetoothAdapter btAdapter, final Context context) {
        super.setContext(gbDevice, btAdapter, context);
        headphoneHelper = new HeadphoneHelper(getContext(), getDevice(), this);
    }

    @Override
    public void onSendConfiguration(final String config) {
        if (!headphoneHelper.onSendConfiguration(config))
            super.onSendConfiguration(config);
    }

    @Override
    public void dispose() {
        synchronized (ConnectionMonitor) {
            if (headphoneHelper != null)
                headphoneHelper.dispose();
            super.dispose();
        }
    }
}
