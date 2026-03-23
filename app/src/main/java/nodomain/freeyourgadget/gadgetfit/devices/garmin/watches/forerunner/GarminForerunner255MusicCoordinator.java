package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.forerunner;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

public class GarminForerunner255MusicCoordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Forerunner 255 Music$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_forerunner_255_music;
    }
}
