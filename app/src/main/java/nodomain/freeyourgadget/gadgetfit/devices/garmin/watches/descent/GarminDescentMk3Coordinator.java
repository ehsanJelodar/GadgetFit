package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.descent;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

public class GarminDescentMk3Coordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Descent Mk3( [\\d+]+mm)?$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_descent_mk3;
    }
}
