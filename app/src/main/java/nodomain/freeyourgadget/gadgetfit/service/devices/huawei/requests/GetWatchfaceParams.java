/*  Copyright (C) 2024 Vitalii Tomin

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
import nodomain.freeyourgadget.gadgetfit.devices.huawei.packets.Watchface;
import nodomain.freeyourgadget.gadgetfit.service.devices.huawei.HuaweiSupportProvider;

public class GetWatchfaceParams extends Request{

    public GetWatchfaceParams(HuaweiSupportProvider support) {
        super(support);
        this.serviceId = Watchface.id;
        this.commandId = Watchface.WatchfaceParams.id;

    }

    @Override
    protected boolean requestSupported() {
        return supportProvider.getDeviceState().supportsWatchfaceParams();
    }

    @Override
    protected List<byte[]> createRequest() throws RequestCreationException {
        try {
            return new Watchface.WatchfaceParams.Request(paramsProvider).serialize();
        } catch (HuaweiPacket.CryptoException e) {
            throw new RequestCreationException(e);
        }
    }

    @Override
    protected void processResponse() throws ResponseParseException {
        if (!(receivedPacket instanceof Watchface.WatchfaceParams.Response))
            throw new ResponseTypeMismatchException(receivedPacket, Watchface.WatchfaceParams.Response.class);

        Watchface.WatchfaceParams.Response resp = (Watchface.WatchfaceParams.Response)(receivedPacket);
        supportProvider.getDeviceState().setWatchfaceDeviceParams(resp.params);
    }
}
