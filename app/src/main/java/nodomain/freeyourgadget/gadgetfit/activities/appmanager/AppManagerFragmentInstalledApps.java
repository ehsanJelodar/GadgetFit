/*  Copyright (C) 2016-2024 Andreas Shimokawa, Arjan Schrijver, Daniele
    Gobbetti

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
package nodomain.freeyourgadget.gadgetfit.activities.appmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import nodomain.freeyourgadget.gadgetfit.impl.GBDeviceApp;
import nodomain.freeyourgadget.gadgetfit.model.DeviceType;

public class AppManagerFragmentInstalledApps extends AbstractAppManagerFragment {

    @Override
    protected List<GBDeviceApp> getSystemAppsInCategory() {
        List<GBDeviceApp> systemApps = new ArrayList<>();
        return systemApps;
    }

    @Override
    protected boolean isCacheManager() {
        return false;
    }

    @Override
    protected String getSortFilename() {
        return mGBDevice.getAddress() + ".watchapps";
    }

    @Override
    protected void onChangedAppOrder() {
        super.onChangedAppOrder();
        sendOrderToDevice(mGBDevice.getAddress() + ".watchfaces");
    }

    @Override
    protected boolean filterApp(GBDeviceApp gbDeviceApp) {
        return gbDeviceApp.getType() == GBDeviceApp.Type.APP_ACTIVITYTRACKER || gbDeviceApp.getType() == GBDeviceApp.Type.APP_GENERIC;
    }
}
