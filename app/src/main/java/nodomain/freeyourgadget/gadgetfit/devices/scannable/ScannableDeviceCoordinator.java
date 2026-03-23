package nodomain.freeyourgadget.gadgetfit.devices.scannable;

import androidx.annotation.NonNull;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.impl.GBDeviceCandidate;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.scannable.ScannableDeviceSupport;

public class ScannableDeviceCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    public boolean supports(GBDeviceCandidate candidate) {
        return false;
    }

    @Override
    public int[] getSupportedDeviceSpecificSettings(GBDevice device) {
        return new int[]{
                R.xml.devicesettings_scannable
        };
    }

    @Override
    public boolean isConnectable() {
        return false;
    }

    @Override
    public String getManufacturer() {
        return "Generic";
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return ScannableDeviceSupport.class;
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_scannable;
    }

    @Override
    public int getBatteryCount(final GBDevice device) {
        return 0;
    }

    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_scannable;
    }

    @Override
    public DeviceCoordinator.DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceCoordinator.DeviceKind.UNKNOWN;
    }
}
