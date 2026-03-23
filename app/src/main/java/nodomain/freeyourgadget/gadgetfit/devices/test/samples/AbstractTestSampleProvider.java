/*  Copyright (C) 2024 José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.devices.test.samples;

import androidx.annotation.Nullable;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.model.TimeSample;

public abstract class AbstractTestSampleProvider<S extends TimeSample> implements TimeSampleProvider<S> {
    @Nullable
    @Override
    public S getLatestSample(final long until) {
        final List<S> allSamples = getAllSamples(until - 60, until);
        return !allSamples.isEmpty() ? allSamples.get(allSamples.size() - 1) : null;
    }

    @Override
    public void addSample(final S timeSample) {
        throw new UnsupportedOperationException("read-only sample provider");
    }

    @Override
    public void addSamples(final List<S> timeSamples) {
        throw new UnsupportedOperationException("read-only sample provider");
    }

    @Override
    public S createSample() {
        throw new UnsupportedOperationException("read-only sample provider");
    }
}
