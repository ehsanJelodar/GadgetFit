package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.instinct;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

public class GarminInstinct2XSolarCoordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        // Allow ending both with "Sol" (#3063) and "Solar" (reported on Matrix).
        return Pattern.compile("^Instinct 2X Sol(ar)?$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_instinct_2x_solar;
    }
}
