package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.instinct;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

import java.util.regex.Pattern;

public class GarminInstinctCrossoverCoordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Instinct Crossover$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_instinct_crossover;
    }
}
