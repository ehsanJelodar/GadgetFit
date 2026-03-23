package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.instinct;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

public class GarminInstinct2XTacticalCoordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Instinct 2X Tactical$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_instinct_2x_tactical;
    }
}
