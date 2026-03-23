/*  Copyright (C) 2018-2024 Daniele Gobbetti, ladbsoft

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
package nodomain.freeyourgadget.gadgetfit.devices.xwatch;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.XWatchActivitySample;
import nodomain.freeyourgadget.gadgetfit.entities.XWatchActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.ActivityKind;

//TODO: extend own class
public class XWatchSampleProvider extends AbstractSampleProvider<XWatchActivitySample> {
    public static final int TYPE_ACTIVITY = -1;

    public XWatchSampleProvider(GBDevice device, DaoSession session) {
        super(device, session);
    }

    @Override
    public ActivityKind normalizeType(int rawType) {
        return ActivityKind.ACTIVITY;
    }

    @Override
    public int toRawActivityKind(ActivityKind activityKind) {
        return TYPE_ACTIVITY;
    }

    @Override
    public float normalizeIntensity(int rawIntensity) {
        return rawIntensity / 180.0f;
    }

    @Override
    public XWatchActivitySample createActivitySample() {
        return new XWatchActivitySample();
    }

    @Override
    public AbstractDao<XWatchActivitySample, ?> getSampleDao() {
        return getSession().getXWatchActivitySampleDao();
    }

    @Nullable
    @Override
    protected Property getRawKindSampleProperty() {
        return XWatchActivitySampleDao.Properties.RawKind;
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return XWatchActivitySampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return XWatchActivitySampleDao.Properties.DeviceId;
    }
}
