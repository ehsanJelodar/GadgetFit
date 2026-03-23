package nodomain.freeyourgadget.gadgetfit.devices.coospo

import nodomain.freeyourgadget.gadgetfit.R
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice
import java.util.regex.Pattern

/// #5025
class CoospoHW807Coordinator: CoospoHeartRateCoordinator() {
    override fun getSupportedDeviceName(): Pattern? {
        return Pattern.compile("^(COOSPO )?HW807$")
    }

    override fun getDeviceNameResource(): Int {
        return R.string.devicetype_coospo_hw807
    }

    override fun getDeviceKind(device: GBDevice): DeviceCoordinator.DeviceKind {
        return DeviceCoordinator.DeviceKind.CHEST_STRAP
    }
}
