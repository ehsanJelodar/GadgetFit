/*  Copyright (C) 2023-2024 José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.service.btle.actions;

import android.annotation.SuppressLint;
import android.bluetooth.BluetoothGatt;

import nodomain.freeyourgadget.gadgetfit.service.btle.BleNamesResolver;
import nodomain.freeyourgadget.gadgetfit.service.btle.BtLEAction;

/// Calls {@link BluetoothGatt#requestConnectionPriority(int)}.
public class RequestConnectionPriorityAction extends BtLEAction {
    private int priority;

    public RequestConnectionPriorityAction(int priority) {
        super(null);
        this.priority = priority;
    }

    @Override
    public boolean expectsResult() {
        return false;
    }

    @Override
    @SuppressLint("MissingPermission")
    public boolean run(final BluetoothGatt gatt) {
        return gatt.requestConnectionPriority(priority);
    }

    @Override
    public String toString() {
        return getCreationTime() + " " + getClass().getSimpleName() + " " +
                BleNamesResolver.getConnectionPriorityString(priority);
    }
}
