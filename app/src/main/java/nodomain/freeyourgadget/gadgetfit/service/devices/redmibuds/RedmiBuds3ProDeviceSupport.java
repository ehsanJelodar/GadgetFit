package nodomain.freeyourgadget.gadgetfit.service.devices.redmibuds;

public class RedmiBuds3ProDeviceSupport extends RedmiBudsDeviceSupport {
    @Override
    protected RedmiBuds3ProProtocol createDeviceProtocol() {
        return new RedmiBuds3ProProtocol(getDevice());
    }
}
