/*  Copyright (C) 2025  Thomas Kuehne

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

package nodomain.freeyourgadget.gadgetfit.devices.ultrahuman.samples;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.UltrahumanActivitySample;
import nodomain.freeyourgadget.gadgetfit.entities.UltrahumanActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.ActivityKind;

public class UltrahumanActivitySampleProvider extends AbstractSampleProvider<UltrahumanActivitySample> {
    public UltrahumanActivitySampleProvider(GBDevice device, DaoSession session) {
        super(device, session);
    }

    public static ActivityKind normalizeTypeStatic(int rawType) {
        return switch (rawType) {
            case 5 -> ActivityKind.BREATHWORK;
            case 6 -> ActivityKind.EXERCISE;
            default -> (rawType >= 100) ? ActivityKind.NOT_WORN : ActivityKind.UNKNOWN;
        };
    }

    public static float normalizeIntensityStatic(int rawIntensity) {
        return rawIntensity / 150.0f;
    }

    @NonNull
    @Override
    public AbstractDao<UltrahumanActivitySample, ?> getSampleDao() {
        return getSession().getUltrahumanActivitySampleDao();
    }

    @Nullable
    @Override
    protected Property getRawKindSampleProperty() {
        return UltrahumanActivitySampleDao.Properties.RawKind;
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return UltrahumanActivitySampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return UltrahumanActivitySampleDao.Properties.DeviceId;
    }

    @Override
    public ActivityKind normalizeType(int rawType) {
        return normalizeTypeStatic(rawType);
    }

    @Override
    public int toRawActivityKind(ActivityKind activityKind) {
        return switch (activityKind) {
            case UNKNOWN -> 1;
            case BREATHWORK -> 5;
            case EXERCISE -> 6;
            case NOT_WORN -> 100;
            default -> 0;
        };
    }

    @Override
    public float normalizeIntensity(int rawIntensity) {
        return normalizeIntensityStatic(rawIntensity);
    }

    @Override
    public UltrahumanActivitySample createActivitySample() {
        return new UltrahumanActivitySample();
    }
}
