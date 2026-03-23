/*  Copyright (C) 2024 Damien Gaignon, Martin.JM

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
package nodomain.freeyourgadget.gadgetfit.service.devices.huawei.requests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSettingsPreferenceConst;
import nodomain.freeyourgadget.gadgetfit.devices.huawei.HuaweiPacket;
import nodomain.freeyourgadget.gadgetfit.devices.huawei.packets.FitnessData;
import nodomain.freeyourgadget.gadgetfit.service.devices.huawei.HuaweiSupportProvider;

public class SetAutomaticSpoRequest extends Request {
    private static final Logger LOG = LoggerFactory.getLogger(SetAutomaticSpoRequest.class);

    public SetAutomaticSpoRequest(HuaweiSupportProvider support) {
        super(support);
        this.serviceId = FitnessData.id;
        this.commandId = FitnessData.EnableAutomaticSpo.id;
        this.addToResponse = false;
    }

    @Override
    protected List<byte[]> createRequest() throws RequestCreationException {
        boolean automaticSpoEnabled = GBApplication
                .getDeviceSpecificSharedPrefs(supportProvider.getDevice().getAddress())
                .getBoolean(DeviceSettingsPreferenceConst.PREF_SPO_AUTOMATIC_ENABLE, false);
        if (automaticSpoEnabled)
            LOG.info("Attempting to enable automatic SpO");
        else
            LOG.info("Attempting to disable automatic SpO");
        try {
            return new FitnessData.EnableAutomaticSpo.Request(paramsProvider, automaticSpoEnabled).serialize();
        } catch (HuaweiPacket.CryptoException e) {
            throw new RequestCreationException(e);
        }
    }
}
