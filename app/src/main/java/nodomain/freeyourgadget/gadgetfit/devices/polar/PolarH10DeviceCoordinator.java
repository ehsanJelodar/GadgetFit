package nodomain.freeyourgadget.gadgetfit.devices.polar;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;

public class PolarH10DeviceCoordinator extends AbstractPolarDeviceCoordinator {
    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_polarh10;
    }

    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Polar H10.*");
    }
}
