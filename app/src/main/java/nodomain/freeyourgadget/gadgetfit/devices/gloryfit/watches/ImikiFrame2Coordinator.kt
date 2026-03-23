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
import java.util.regex.Pattern

class ImikiFrame2Coordinator : GloryFitCoordinator() {
    override fun isExperimental(): Boolean {
        return true
    }

    override fun getManufacturer(): String {
        return "IMIKI"
    }

    override fun getSupportedDeviceName(): Pattern? {
        return Pattern.compile("^IMIKI FRAME 2$")
    }

    override fun getDeviceNameResource(): Int {
        return R.string.devicetype_imiki_frame_2
    }
}
