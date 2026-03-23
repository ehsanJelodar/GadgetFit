/*  Copyright (C) 2020-2025 Andreas Shimokawa, Damien Gaignon, Daniel Dakhno,
    Davis Mosenkovs, ITCactus, José Rebelo, Patric Gruber, Petr Vaněk, Taavi
    Eomäe, uli, Thomas Kuehne

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
package nodomain.freeyourgadget.gadgetfit.devices.pinetime;

import android.content.Context;
import android.net.Uri;

import androidx.annotation.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.InstallHandler;
import nodomain.freeyourgadget.gadgetfit.devices.SampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.PineTimeActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.ActivitySample;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.pinetime.PineTimeJFSupport;

public class PineTimeJFCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("Pinetime-JF.*|InfiniTime.*");
    }

    @Override
    public InstallHandler findInstallHandler(Uri uri, Context context) {
        PineTimeInstallHandler handler = new PineTimeInstallHandler(uri, context);
        return handler.isValid() ? handler : null;
    }

    @Override
    public boolean supportsFlashing(@NonNull GBDevice device) { return true; }

    @Override
    public boolean supportsActivityTracking(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public SampleProvider<? extends ActivitySample> getSampleProvider(GBDevice device, DaoSession session) {
        return new PineTimeActivitySampleProvider(device, session);
    }

    @Override
    public boolean supportsHeartRateMeasurement(GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsManualHeartRateMeasurement(GBDevice device) {
        return false;
    }

    @Override
    public String getManufacturer() {
        return "Pine64";
    }

    @Override
    public boolean supportsWeather(final GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsFindDevice(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsMusicInfo(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public int getWorldClocksSlotCount() {
        return 4;
    }

    @Override
    public int getWorldClocksLabelLength() {
        return 8;
    }

    @Override
    public boolean supportsNavigation(final GBDevice device) {
        return true;
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return PineTimeJFSupport.class;
    }

    @Override
    public int[] getSupportedDeviceSpecificSettings(GBDevice device) {
        return new int[]{
                R.xml.devicesettings_transliteration,
                R.xml.devicesettings_world_clocks,
                R.xml.devicesettings_prefix_notification_with_app
        };
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_pinetime_jf;
    }


    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_pinetime;
    }

    @Override
    public Map<AbstractDao<?, ?>, Property> getAllDeviceDao(@NonNull final DaoSession session) {
        Map<AbstractDao<?, ?>, Property> map = new HashMap<>(1);
        map.put(session.getPineTimeActivitySampleDao(), PineTimeActivitySampleDao.Properties.DeviceId);
        return map;
    }

    @Override
    public DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceKind.WATCH;
    }
}
