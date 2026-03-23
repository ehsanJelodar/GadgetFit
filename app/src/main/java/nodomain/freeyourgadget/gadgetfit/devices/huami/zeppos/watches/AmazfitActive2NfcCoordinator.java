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
package nodomain.freeyourgadget.gadgetfit.devices.huami.zeppos.watches;

import java.util.Collections;
import java.util.List;

import nodomain.freeyourgadget.gadgetfit.R;

/**
 * This seems to be the same as the Active 2, but with NFC. The bluetooth name is different, but the
 * device sources seem to be similar.
 */
public class AmazfitActive2NfcCoordinator extends AmazfitActive2RoundCoordinator {
    @Override
    public List<String> getDeviceBluetoothNames() {
        // FIXME migrate device type to normal active 2
        return Collections.singletonList("Active 2 NFC (Round)");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_amazfit_active_2_nfc;
    }
}
