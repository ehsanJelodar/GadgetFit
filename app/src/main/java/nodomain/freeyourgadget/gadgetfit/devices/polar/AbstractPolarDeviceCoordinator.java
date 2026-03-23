/*  Copyright (C) 2024-2025 José Rebelo, g_p, Martin.JM, Thomas Kuehne

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
package nodomain.freeyourgadget.gadgetfit.devices.polar;

import androidx.annotation.NonNull;

import java.util.HashMap;
import java.util.Map;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.SampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.HeartRrIntervalSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.PolarH10ActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.ActivitySample;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.polar.PolarH10DeviceSupport;

public abstract class AbstractPolarDeviceCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_lovetoy;
    }

    @Override
    public String getManufacturer() {
        return "Polar";
    }

    @Override
    public int getBondingStyle() {
        return BONDING_STYLE_NONE;
    }

    @Override
    public boolean supportsHeartRateMeasurement(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsCharts(@NonNull GBDevice device) {
        return true;
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(GBDevice device) {
        return PolarH10DeviceSupport.class;
    }

    @Override
    public SampleProvider<? extends ActivitySample> getSampleProvider(GBDevice device, DaoSession session) {
        return new PolarH10ActivitySampleProvider(device, session);
    }

    @Override
    public Map<AbstractDao<?, ?>, Property> getAllDeviceDao(@NonNull final DaoSession session) {
        Map<AbstractDao<?, ?>, Property> map = new HashMap<>(1);
        map.put(session.getPolarH10ActivitySampleDao(), PolarH10ActivitySampleDao.Properties.DeviceId);
        map.put(session.getHeartRrIntervalSampleDao(), HeartRrIntervalSampleDao.Properties.DeviceId);
        return map;
    }

    @Override
    public DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceKind.CHEST_STRAP;
    }
}
