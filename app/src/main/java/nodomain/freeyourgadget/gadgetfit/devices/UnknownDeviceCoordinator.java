/*  Copyright (C) 2015-2024 Andreas Shimokawa, Carsten Pfeiffer, Damien
    Gaignon, Daniel Dakhno, Daniele Gobbetti, José Rebelo, Petr Vaněk

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
package nodomain.freeyourgadget.gadgetfit.devices;

import android.app.Activity;

import androidx.annotation.DrawableRes;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.StringRes;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.ControlCenterv2;
import nodomain.freeyourgadget.gadgetfit.entities.AbstractActivitySample;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.impl.GBDeviceCandidate;
import nodomain.freeyourgadget.gadgetfit.model.ActivityKind;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.unknown.UnknownDeviceSupport;

public class UnknownDeviceCoordinator extends AbstractDeviceCoordinator {
    private final UnknownSampleProvider sampleProvider;

    private static final class UnknownSampleProvider implements SampleProvider<AbstractActivitySample> {
        @Override
        public ActivityKind normalizeType(int rawType) {
            return ActivityKind.UNKNOWN;
        }

        @Override
        public int toRawActivityKind(ActivityKind activityKind) {
            return 0;
        }

        @Override
        public float normalizeIntensity(int rawIntensity) {
            return 0;
        }

        @Override
        public List<AbstractActivitySample> getAllActivitySamples(int timestamp_from, int timestamp_to) {
            return null;
        }

        @Override
        public List<AbstractActivitySample> getAllActivitySamplesHighRes(int timestamp_from, int timestamp_to) {
            return null;
        }

        @Override
        public boolean hasHighResData() {
            return false;
        }

        @Override
        public List<AbstractActivitySample> getActivitySamples(int timestamp_from, int timestamp_to) {
            return null;
        }

        @Override
        public void addGBActivitySample(AbstractActivitySample activitySample) {
        }

        @Override
        public void addGBActivitySamples(AbstractActivitySample[] activitySamples) {
        }

        @Override
        public AbstractActivitySample createActivitySample() {
            return null;
        }

        @Nullable
        @Override
        public AbstractActivitySample getLatestActivitySample() {
            return null;
        }

        @Nullable
        @Override
        public AbstractActivitySample getLatestActivitySample(final int until) {
            return null;
        }

        @Nullable
        @Override
        public AbstractActivitySample getFirstActivitySample() {
            return null;
        }

    }

    public UnknownDeviceCoordinator() {
        sampleProvider = new UnknownSampleProvider();
    }

    @Override
    public boolean supports(@NonNull GBDeviceCandidate candidate) {
        return false;
    }

    @Override
    public Class<? extends Activity> getPairingActivity() {
        return ControlCenterv2.class;
    }

    @Override
    public SampleProvider<?> getSampleProvider(GBDevice device, DaoSession session) {
        return sampleProvider;
    }

    @Override
    public String getManufacturer() {
        return "Generic";
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return UnknownDeviceSupport.class;
    }

    @Override
    @StringRes
    public int getDeviceNameResource() {
        return R.string.devicetype_unknown;
    }

    @Override
    @DrawableRes
    public int getDefaultIconResource() {
        return R.drawable.ic_device_unknown;
    }

    @Override
    public DeviceCoordinator.DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceCoordinator.DeviceKind.UNKNOWN;
    }
}
