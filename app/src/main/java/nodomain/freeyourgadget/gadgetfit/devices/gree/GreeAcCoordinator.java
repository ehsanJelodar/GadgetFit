package nodomain.freeyourgadget.gadgetfit.devices.gree;

import android.app.Activity;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.gree.GreeAcSupport;

public class GreeAcCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        // GR-AC_10001_09_xxxx_SC
        return Pattern.compile("^GR-AC_\\d{5}_\\d{2}_[0-9a-f]{4}_SC$");
    }

    @Override
    public String getManufacturer() {
        return "Gree";
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return GreeAcSupport.class;
    }

    @Override
    public int getBondingStyle() {
        return BONDING_STYLE_NONE;
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_gree_ac;
    }

    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_air_conditioning;
    }

    @Override
    public boolean suggestUnbindBeforePair() {
        // shouldn't matter
        return false;
    }

    @Nullable
    @Override
    public Class<? extends Activity> getPairingActivity() {
        return GreeAcPairingActivity.class;
    }

    @Override
    public DeviceCoordinator.DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceCoordinator.DeviceKind.UNKNOWN;
    }
}
