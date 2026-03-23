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
package nodomain.freeyourgadget.gadgetfit.devices.xiaomi;

import nodomain.freeyourgadget.gadgetfit.devices.AbstractSampleToTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.XiaomiActivitySample;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.BodyEnergySample;

public class XiaomiBodyEnergySampleProvider extends AbstractSampleToTimeSampleProvider<BodyEnergySample, XiaomiActivitySample> {
    public XiaomiBodyEnergySampleProvider(final GBDevice device, final DaoSession session) {
        super(new XiaomiSampleProvider(device, session), device, session);
    }

    @Override
    protected BodyEnergySample convertSample(final XiaomiActivitySample sample) {
        if (sample.getEnergy() <= 0) {
            return null;
        }

        return new XiaomiBodyEnergySample(
                sample.getTimestamp() * 1000L,
                sample.getEnergy()
        );
    }

    protected static class XiaomiBodyEnergySample implements BodyEnergySample {
        private final long timestamp;
        private final int energy;

        public XiaomiBodyEnergySample(final long timestamp, final int energy) {
            this.timestamp = timestamp;
            this.energy = energy;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public int getEnergy() {
            return energy;
        }
    }
}
