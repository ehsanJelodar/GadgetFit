package nodomain.freeyourgadget.gadgetfit.service.devices.earfun.airs;

import nodomain.freeyourgadget.gadgetfit.service.devices.earfun.EarFunDeviceSupport;

public class EarFunAirSDeviceSupport extends EarFunDeviceSupport {

    @Override
    protected EarFunAirSProtocol createDeviceProtocol() {
        return new EarFunAirSProtocol(getDevice());
    }
}
