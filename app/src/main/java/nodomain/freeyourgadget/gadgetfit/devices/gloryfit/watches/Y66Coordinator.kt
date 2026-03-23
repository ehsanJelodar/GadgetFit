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
package nodomain.freeyourgadget.gadgetfit.devices.gloryfit.watches

import nodomain.freeyourgadget.gadgetfit.R
import nodomain.freeyourgadget.gadgetfit.devices.gloryfit.GloryFitCoordinator
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice
import java.util.regex.Pattern

class Y66Coordinator : GloryFitCoordinator() {
    override fun getManufacturer(): String {
        return "GloryFit"
    }

    override fun getSupportedDeviceName(): Pattern? {
        return Pattern.compile("^Y66\\(ID-[0-9A-F]{4}\\)$")
    }

    override fun getDeviceNameResource(): Int {
        return R.string.devicetype_y66
    }

    override fun getAlarmSlotCount(device: GBDevice): Int {
        // alarms from app are not supported as per #5063
        return 0
    }
}
