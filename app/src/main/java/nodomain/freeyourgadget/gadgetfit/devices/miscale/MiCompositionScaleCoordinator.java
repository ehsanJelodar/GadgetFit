/*  Copyright (C) 2019-2024 Andreas Shimokawa, Damien Gaignon, Daniel Dakhno,
    Jean-François Greffier, José Rebelo, Petr Vaněk

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
package nodomain.freeyourgadget.gadgetfit.devices.miscale;

import android.bluetooth.le.ScanFilter;
import android.os.ParcelUuid;

import androidx.annotation.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.MiScaleWeightSampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.WeightSample;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.btle.GattService;
import nodomain.freeyourgadget.gadgetfit.service.devices.miscale.MiCompositionScaleDeviceSupport;

public class MiCompositionScaleCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    public Map<AbstractDao<?, ?>, Property> getAllDeviceDao(@NonNull final DaoSession session) {
        Map<AbstractDao<?, ?>, Property> map = new HashMap<>(1);
        map.put(session.getMiScaleWeightSampleDao(), MiScaleWeightSampleDao.Properties.DeviceId);
        return map;
    }

    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("MIBCS|MIBFS", Pattern.CASE_INSENSITIVE);
    }

    @NonNull
    @Override
    public Collection<? extends ScanFilter> createBLEScanFilters() {
        final ParcelUuid bodyCompositionService = new ParcelUuid(GattService.UUID_SERVICE_BODY_COMPOSITION);

        final ScanFilter.Builder builder = new ScanFilter.Builder();
        builder.setServiceUuid(bodyCompositionService);

        final int manufacturerId = 0x0157; // Huami
        builder.setManufacturerData(manufacturerId, new byte[6], new byte[6]);

        return Collections.singletonList(builder.build());
    }

    @Override
    public int getBatteryCount(final GBDevice device) {
        return 0;
    }

    @Override
    public int getBondingStyle() {
        return BONDING_STYLE_NONE;
    }

    @Override
    public String getManufacturer() {
        // Actual manufacturer is Huami
        return "Xiaomi";
    }

    @Override
    public TimeSampleProvider<? extends WeightSample> getWeightSampleProvider(final GBDevice device, final DaoSession session) {
        return new MiScaleSampleProvider(device, session);
    }

    @Override
    public boolean supportsWeightMeasurement(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsCharts(@NonNull GBDevice device) {
        return true;
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return MiCompositionScaleDeviceSupport.class;
    }

    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_micompositionscale;
    }

    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_miscale;
    }

    @Override
    public DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceKind.SCALE;
    }
}
