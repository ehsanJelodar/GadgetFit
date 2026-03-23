/*  Copyright (C) 2023-2024 Johannes Krude

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
package nodomain.freeyourgadget.gadgetfit.devices.casio.gbx100;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.CasioGBX100ActivitySample;
import nodomain.freeyourgadget.gadgetfit.entities.CasioGBX100ActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.ActivityKind;

public class CasioGBX100SampleProvider extends AbstractSampleProvider<CasioGBX100ActivitySample> {
    public CasioGBX100SampleProvider(GBDevice device, DaoSession session) {
        super(device, session);
    }

    @Override
    public ActivityKind normalizeType(int rawType) {
        return ActivityKind.fromCode(rawType);
    }

    @Override
    public int toRawActivityKind(ActivityKind activityKind) {
        return activityKind.getCode();
    }

    @Override
    public float normalizeIntensity(int rawIntensity) {
        // The magic number 1500 is based on
        // https://www.livestrong.com/article/474836-what-sport-burns-the-most-calories-per-hour/
        return (rawIntensity / 1500f);
    }

    @Override
    public CasioGBX100ActivitySample createActivitySample() {
        return new CasioGBX100ActivitySample();
    }

    @Override
    public AbstractDao<CasioGBX100ActivitySample, ?> getSampleDao() {
        return getSession().getCasioGBX100ActivitySampleDao();
    }

    @Nullable
    @Override
    protected Property getRawKindSampleProperty() {
        return CasioGBX100ActivitySampleDao.Properties.RawKind;
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return CasioGBX100ActivitySampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return CasioGBX100ActivitySampleDao.Properties.DeviceId;
    }
}
