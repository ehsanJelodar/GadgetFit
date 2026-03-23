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
import nodomain.freeyourgadget.gadgetfit.devices.huawei.packets.FitnessData;
import nodomain.freeyourgadget.gadgetfit.service.devices.huawei.HuaweiSupportProvider;

public class GetFitnessTotalsRequest extends Request {

    public GetFitnessTotalsRequest(HuaweiSupportProvider support) {
        super(support);

        this.serviceId = FitnessData.id;
        this.commandId = FitnessData.FitnessTotals.id;
    }

    @Override
    protected List<byte[]> createRequest() throws RequestCreationException {
        try {
            return new FitnessData.FitnessTotals.Request(
                    paramsProvider
            ).serialize();
        } catch (HuaweiPacket.CryptoException e) {
            throw new RequestCreationException(e);
        }
    }

    @Override
    protected void processResponse() throws ResponseParseException {
        if (!(receivedPacket instanceof FitnessData.FitnessTotals.Response))
            throw new ResponseTypeMismatchException(receivedPacket, FitnessData.FitnessTotals.Response.class);

        int totalSteps = ((FitnessData.FitnessTotals.Response) receivedPacket).totalSteps;
        int totalCalories = ((FitnessData.FitnessTotals.Response) receivedPacket).totalCalories;
        int totalDistance = ((FitnessData.FitnessTotals.Response) receivedPacket).totalDistance;

        supportProvider.addTotalFitnessData(totalSteps, totalCalories, totalDistance);
    }
}
