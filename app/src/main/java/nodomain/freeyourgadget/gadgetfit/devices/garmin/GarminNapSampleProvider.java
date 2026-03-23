/*  Copyright (C) 2025 José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.devices.garmin;

import androidx.annotation.NonNull;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.GarminNapSample;
import nodomain.freeyourgadget.gadgetfit.entities.GarminNapSampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class GarminNapSampleProvider extends AbstractTimeSampleProvider<GarminNapSample> {
    public GarminNapSampleProvider(final GBDevice device, final DaoSession session) {
        super(device, session);
    }

    @NonNull
    @Override
    public AbstractDao<GarminNapSample, ?> getSampleDao() {
        return getSession().getGarminNapSampleDao();
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return GarminNapSampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return GarminNapSampleDao.Properties.DeviceId;
    }

    @Override
    public GarminNapSample createSample() {
        return new GarminNapSample();
    }
}
