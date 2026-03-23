package nodomain.freeyourgadget.gadgetfit.devices.gloryfit.watches

import nodomain.freeyourgadget.gadgetfit.R
import nodomain.freeyourgadget.gadgetfit.devices.gloryfit.GloryFitCoordinator
import java.util.regex.Pattern

class R1Coordinator : GloryFitCoordinator() {
    override fun getManufacturer(): String {
        return "GloryFit"
    }

    override fun getSupportedDeviceName(): Pattern? {
        return Pattern.compile("^R1\\(ID-[0-9A-F]{4}\\)$")
    }

    override fun getDeviceNameResource(): Int {
        return R.string.devicetype_r1
    }
}
