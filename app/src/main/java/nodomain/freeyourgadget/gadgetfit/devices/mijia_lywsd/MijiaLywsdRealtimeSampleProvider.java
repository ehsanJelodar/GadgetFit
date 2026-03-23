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
    along with this program.  If not, see <http://www.gnu.org/licenses/>. */
package nodomain.freeyourgadget.gadgetfit.devices.mijia_lywsd;

import androidx.annotation.NonNull;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.MijiaLywsdRealtimeSample;
import nodomain.freeyourgadget.gadgetfit.entities.MijiaLywsdRealtimeSampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.TemperatureSample;

public class MijiaLywsdRealtimeSampleProvider extends AbstractTimeSampleProvider<MijiaLywsdRealtimeSample> {
    public MijiaLywsdRealtimeSampleProvider(final GBDevice device, final DaoSession session) {
        super(device, session);
    }

    @NonNull
    @Override
    public AbstractDao<MijiaLywsdRealtimeSample, ?> getSampleDao() {
        return getSession().getMijiaLywsdRealtimeSampleDao();
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return MijiaLywsdRealtimeSampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return MijiaLywsdRealtimeSampleDao.Properties.DeviceId;
    }

    @Override
    public MijiaLywsdRealtimeSample createSample() {
        MijiaLywsdRealtimeSample sample = new MijiaLywsdRealtimeSample();
        sample.setTemperatureType(TemperatureSample.TYPE_AMBIENT);
        sample.setTemperatureLocation(TemperatureSample.LOCATION_UNKNOWN);
        return sample;
    }
}
