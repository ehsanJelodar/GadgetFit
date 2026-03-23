package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.enduro;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

public class GarminEnduroCoordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Enduro$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_enduro;
    }
}
