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

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.devices.huawei.HuaweiPacket;
import nodomain.freeyourgadget.gadgetfit.devices.huawei.packets.FindPhone;
import nodomain.freeyourgadget.gadgetfit.service.devices.huawei.HuaweiSupportProvider;

public class StopFindPhoneRequest extends Request {
    public StopFindPhoneRequest(HuaweiSupportProvider support) {
        super(support);
        this.serviceId = FindPhone.id;
        this.commandId = FindPhone.StopRequest.id;
    }

    @Override
    protected List<byte[]> createRequest() throws RequestCreationException {
        try {
            return new FindPhone.StopRequest(paramsProvider).serialize();
        } catch (HuaweiPacket.CryptoException e) {
            throw new RequestCreationException(e);
        }
    }
}
