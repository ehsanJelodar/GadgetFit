package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.venu;

import androidx.annotation.NonNull;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class GarminVenu3Coordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Venu 3$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_venu_3;
    }

    @Override
    public boolean supportsTrainingLoad(@NonNull GBDevice device) {
        return false;
    }

    @Override
    public boolean supportsVO2MultiSport(@NonNull final GBDevice device) {
        return false;
    }
}
