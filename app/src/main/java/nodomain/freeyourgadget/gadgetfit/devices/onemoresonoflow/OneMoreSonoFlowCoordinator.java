package nodomain.freeyourgadget.gadgetfit.devices.onemoresonoflow;

import androidx.annotation.NonNull;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettings;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettingsScreen;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLClassicDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.BatteryConfig;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.onemore_sonoflow.OneMoreSonoFlowSupport;

public class OneMoreSonoFlowCoordinator extends AbstractBLClassicDeviceCoordinator {

    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("1MORE SonoFlow");
    }

    @Override
    public String getManufacturer() {
        return "1MORE";
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return OneMoreSonoFlowSupport.class;
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_onemore_sonoflow;
    }

    @Override
    public BatteryConfig[] getBatteryConfig(final GBDevice device) {
        return new BatteryConfig[] {
            new BatteryConfig(
                0,
                GBDevice.BATTERY_ICON_DEFAULT,
                GBDevice.BATTERY_LABEL_DEFAULT,
                20,
                100
            )
        };
    }

    @Override
    public DeviceSpecificSettings getDeviceSpecificSettings(final GBDevice device) {
        final DeviceSpecificSettings settings = new DeviceSpecificSettings();

        settings.addRootScreen(DeviceSpecificSettingsScreen.SOUND);
        settings.addSubScreen(
            DeviceSpecificSettingsScreen.SOUND,
            R.xml.devicesettings_onemore_noise_control_selector
        );
        settings.addSubScreen(
            DeviceSpecificSettingsScreen.SOUND,
            R.xml.devicesettings_ldac_toggle
        );
        settings.addRootScreen(
            DeviceSpecificSettingsScreen.CONNECTION,
            R.xml.devicesettings_dual_device_toggle
        );

        settings.addRootScreen(DeviceSpecificSettingsScreen.CALLS_AND_NOTIFICATIONS);
        settings.addSubScreen(
            DeviceSpecificSettingsScreen.CALLS_AND_NOTIFICATIONS,
            R.xml.devicesettings_headphones
        );

        return settings;
    }

    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_headphones;
    }

    @Override
    public DeviceCoordinator.DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceKind.HEADPHONES;
    }
}
