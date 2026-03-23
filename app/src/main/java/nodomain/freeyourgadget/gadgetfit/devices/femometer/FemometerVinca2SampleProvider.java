/*  Copyright (C) 2023-2024 Alicia Hormann

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
package nodomain.freeyourgadget.gadgetfit.devices.femometer;

import androidx.annotation.NonNull;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.FemometerVinca2TemperatureSample;
import nodomain.freeyourgadget.gadgetfit.entities.FemometerVinca2TemperatureSampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.TemperatureSample;

public class FemometerVinca2SampleProvider extends AbstractTimeSampleProvider<FemometerVinca2TemperatureSample> {

    public FemometerVinca2SampleProvider(GBDevice device, DaoSession session) {
        super(device, session);
    }

    @Override
    @NonNull
    public AbstractDao<FemometerVinca2TemperatureSample, ?> getSampleDao() {
        return getSession().getFemometerVinca2TemperatureSampleDao();
    }

    @Override
    @NonNull
    protected Property getTimestampSampleProperty() {
        return FemometerVinca2TemperatureSampleDao.Properties.Timestamp;
    }

    @Override
    @NonNull
    protected Property getDeviceIdentifierSampleProperty() {
        return FemometerVinca2TemperatureSampleDao.Properties.DeviceId;
    }

    @Override
    public FemometerVinca2TemperatureSample createSample() {
        FemometerVinca2TemperatureSample sample = new FemometerVinca2TemperatureSample();
        sample.setTemperatureType(TemperatureSample.TYPE_BODY);
        sample.setTemperatureLocation(TemperatureSample.LOCATION_MOUTH);
        return sample;
    }
}
