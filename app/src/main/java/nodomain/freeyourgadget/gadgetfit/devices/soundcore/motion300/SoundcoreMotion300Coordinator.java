/*  Copyright (C) 2024 Severin von Wnuck-Lipinski

    This file is part of gadgetbridge.

    gadgetbridge is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    gadgetbridge is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>. */
package nodomain.freeyourgadget.gadgetfit.devices.soundcore.motion300;

import androidx.annotation.NonNull;

import java.util.regex.Pattern;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettings;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettingsCustomizer;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettingsScreen;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLClassicDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.soundcore.motion300.SoundcoreMotion300DeviceSupport;

public class SoundcoreMotion300Coordinator extends AbstractBLClassicDeviceCoordinator {
    @Override
    public String getManufacturer() {
        return "Anker";
    }

    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("soundcore Motion 300");
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_soundcore_motion300;
    }

    @Override
    public DeviceSpecificSettingsCustomizer getDeviceSpecificSettingsCustomizer(final GBDevice device) {
        return new SoundcoreMotion300SettingsCustomizer();
    }

    @Override
    public boolean supportsPowerOff(@NonNull final GBDevice device) {
        return true;
    }

    @Override
    public DeviceSpecificSettings getDeviceSpecificSettings(final GBDevice device) {
        final DeviceSpecificSettings settings = new DeviceSpecificSettings();

        settings.addRootScreen(R.xml.devicesettings_soundcore_motion300);
        settings.addRootScreen(DeviceSpecificSettingsScreen.AUDIO);
        settings.addSubScreen(
                DeviceSpecificSettingsScreen.AUDIO,
                R.xml.devicesettings_soundcore_motion300_audio);
        settings.addRootScreen(DeviceSpecificSettingsScreen.CALLS_AND_NOTIFICATIONS);
        settings.addSubScreen(
                DeviceSpecificSettingsScreen.CALLS_AND_NOTIFICATIONS,
                R.xml.devicesettings_headphones);

        return settings;
    }

    @Override
    public DeviceCoordinator.DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceKind.SPEAKER;
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return SoundcoreMotion300DeviceSupport.class;
    }
}
