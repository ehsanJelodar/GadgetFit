/*  Copyright (C) 2023-2024 a0z

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
package nodomain.freeyourgadget.gadgetfit.activities.dashboard;

import android.os.Bundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.DashboardFragment;
import nodomain.freeyourgadget.gadgetfit.database.DBHandler;
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.SleepScoreSample;

/**
 * A simple {@link AbstractDashboardWidget} subclass.
 * Use the {@link DashboardSleepScoreWidget#newInstance} factory method to
 * create an instance of this fragment.
 */
public class DashboardSleepScoreWidget extends AbstractGaugeWidget {
    protected static final Logger LOG = LoggerFactory.getLogger(DashboardSleepScoreWidget.class);
    public DashboardSleepScoreWidget() {
        super(R.string.sleep_score, "sleep");
    }

    /**
     * Use this factory method to create a new instance of
     * this fragment using the provided parameters.
     *
     * @param dashboardData An instance of DashboardFragment.DashboardData.
     * @return A new instance of fragment DashboardSleepScoreWidget.
     */
    public static DashboardSleepScoreWidget newInstance(final DashboardFragment.DashboardData dashboardData) {
        final DashboardSleepScoreWidget fragment = new DashboardSleepScoreWidget();
        final Bundle args = new Bundle();
        args.putSerializable(ARG_DASHBOARD_DATA, dashboardData);
        fragment.setArguments(args);
        return fragment;
    }

    @Override
    protected void populateData(final DashboardFragment.DashboardData dashboardData) {
        final List<GBDevice> devices = getSupportedDevices(dashboardData);
        final SleepScoreData data = new SleepScoreData();

        SleepScoreSample sample = null;
        try (DBHandler dbHandler = GBApplication.acquireDB()) {
            for (GBDevice dev : devices) {
                TimeSampleProvider<? extends SleepScoreSample> provider = dev.getDeviceCoordinator().getSleepScoreProvider(dev, dbHandler.getDaoSession());
                final SleepScoreSample latestSample = provider.getLatestSample(dashboardData.timeTo * 1000L);
                if (latestSample != null && (sample == null || latestSample.getTimestamp() > sample.getTimestamp())) {
                    sample = latestSample;
                }
            }

            if (sample != null) {
                data.value = sample.getSleepScore();
            }

        } catch (final Exception e) {
            LOG.error("Could not get vo2max for today", e);
        }

        dashboardData.put("sleepscore", data);
    }

    @Override
    protected void draw(final DashboardFragment.DashboardData dashboardData) {
        final SleepScoreData data = (SleepScoreData) dashboardData.get("sleepscore");
        setText(String.valueOf(data.value));
        drawSimpleGauge(
                color_light_sleep,
                (float) data.value / 100
        );
    }

    @Override
    protected boolean isSupportedBy(final GBDevice device) {
        return device.getDeviceCoordinator().supportsSleepScore(device);
    }

    private static class SleepScoreData implements Serializable {
        public int value = -1;
    }
}
