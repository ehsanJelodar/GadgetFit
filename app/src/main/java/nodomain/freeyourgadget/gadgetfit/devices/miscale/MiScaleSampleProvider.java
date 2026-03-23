/*  Copyright (C) 2024 Severin von Wnuck-Lipinski

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

import androidx.annotation.NonNull;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.MiScaleWeightSample;
import nodomain.freeyourgadget.gadgetfit.entities.MiScaleWeightSampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class MiScaleSampleProvider extends AbstractTimeSampleProvider<MiScaleWeightSample> {
    public MiScaleSampleProvider(GBDevice device, DaoSession session) {
        super(device, session);
    }

    @Override
    @NonNull
    public AbstractDao<MiScaleWeightSample, ?> getSampleDao() {
        return getSession().getMiScaleWeightSampleDao();
    }

    @Override
    @NonNull
    protected Property getTimestampSampleProperty() {
        return MiScaleWeightSampleDao.Properties.Timestamp;
    }

    @Override
    @NonNull
    protected Property getDeviceIdentifierSampleProperty() {
        return MiScaleWeightSampleDao.Properties.DeviceId;
    }

    @Override
    public MiScaleWeightSample createSample() {
        return new MiScaleWeightSample();
    }
}
