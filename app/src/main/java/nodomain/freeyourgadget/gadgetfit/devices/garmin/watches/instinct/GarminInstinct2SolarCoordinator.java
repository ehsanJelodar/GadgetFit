package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.instinct;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

import java.util.regex.Pattern;

public class GarminInstinct2SolarCoordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Instinct 2 Solar$");
    }
    
    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_instinct_2_solar;
    }
}
