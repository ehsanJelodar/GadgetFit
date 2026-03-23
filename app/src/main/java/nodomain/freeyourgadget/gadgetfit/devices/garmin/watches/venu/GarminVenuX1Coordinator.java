package nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.venu;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.watches.GarminWatchCoordinator;

public class GarminVenuX1Coordinator extends GarminWatchCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^Venu X1$");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_garmin_venu_x1;
    }

    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_amazfit_bip;
    }

    @Override
    public boolean defaultNewSyncProtocol() {
        return true;
    }
}
