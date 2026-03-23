package nodomain.freeyourgadget.gadgetfit.devices.coospo

import nodomain.freeyourgadget.gadgetfit.R
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice
import java.util.regex.Pattern

/// #5110
class CoospoHW9Coordinator: CoospoHeartRateCoordinator() {
    override fun getSupportedDeviceName(): Pattern? {
        return Pattern.compile("^HW9 [0-9]{5}$")
    }

    override fun getDeviceNameResource(): Int {
        return R.string.devicetype_coospo_hw9
    }

    override fun getDeviceKind(device: GBDevice): DeviceCoordinator.DeviceKind {
        return DeviceCoordinator.DeviceKind.CHEST_STRAP
    }
}
