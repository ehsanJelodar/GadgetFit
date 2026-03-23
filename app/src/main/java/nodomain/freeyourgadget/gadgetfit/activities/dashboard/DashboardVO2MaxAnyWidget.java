/*  Copyright (C) 2024-2025 a0z, José Rebelo, Thomas Kuehne

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
package nodomain.freeyourgadget.gadgetfit.activities.dashboard;

import android.os.Bundle;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.DashboardFragment;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.Vo2MaxSample;

public class DashboardVO2MaxAnyWidget extends AbstractDashboardVO2MaxWidget {

    public DashboardVO2MaxAnyWidget() {
        super(R.string.menuitem_vo2_max, "vo2max");
    }

    public static DashboardVO2MaxAnyWidget newInstance(final DashboardFragment.DashboardData dashboardData) {
        final DashboardVO2MaxAnyWidget fragment = new DashboardVO2MaxAnyWidget();
        final Bundle args = new Bundle();
        args.putSerializable(ARG_DASHBOARD_DATA, dashboardData);
        fragment.setArguments(args);
        return fragment;
    }

    public Vo2MaxSample.Type getVO2MaxType() {
        return Vo2MaxSample.Type.ANY;
    }

    public String getWidgetKey() {
        return "vo2max";
    }

    @Override
    protected boolean isSupportedBy(final GBDevice device) {
        return device.getDeviceCoordinator().supportsVO2Max(device);
    }
}
