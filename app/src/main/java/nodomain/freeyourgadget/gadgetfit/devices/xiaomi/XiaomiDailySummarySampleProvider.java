/*  Copyright (C) 2023-2024 José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.devices.xiaomi;

import androidx.annotation.NonNull;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.XiaomiDailySummarySample;
import nodomain.freeyourgadget.gadgetfit.entities.XiaomiDailySummarySampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class XiaomiDailySummarySampleProvider extends AbstractTimeSampleProvider<XiaomiDailySummarySample> {
    public XiaomiDailySummarySampleProvider(final GBDevice device, final DaoSession session) {
        super(device, session);
    }

    @NonNull
    @Override
    public AbstractDao<XiaomiDailySummarySample, ?> getSampleDao() {
        return getSession().getXiaomiDailySummarySampleDao();
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return XiaomiDailySummarySampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return XiaomiDailySummarySampleDao.Properties.DeviceId;
    }

    @Override
    public XiaomiDailySummarySample createSample() {
        return new XiaomiDailySummarySample();
    }
}
