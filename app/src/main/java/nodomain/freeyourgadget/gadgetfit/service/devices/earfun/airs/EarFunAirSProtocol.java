package nodomain.freeyourgadget.gadgetfit.service.devices.earfun.airs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.DeviceType;
import nodomain.freeyourgadget.gadgetfit.service.devices.earfun.EarFunPacketEncoder;
import nodomain.freeyourgadget.gadgetfit.service.devices.earfun.EarFunProtocol;
import nodomain.freeyourgadget.gadgetfit.service.devices.earfun.prefs.Equalizer;
import nodomain.freeyourgadget.gadgetfit.util.Prefs;

public class EarFunAirSProtocol extends EarFunProtocol {
    private static final Logger LOG = LoggerFactory.getLogger(EarFunAirSProtocol.class);

    @Override
    public byte[] encodeSendConfiguration(String config) {
        if (Equalizer.containsKey(Equalizer.SixBandEqualizer, config)) {
            Prefs prefs = getDevicePrefs();
            return EarFunPacketEncoder.encodeSetEqualizerSixBands(prefs);
        }
        return super.encodeSendConfiguration(config);
    }

    protected EarFunAirSProtocol(GBDevice device) {
        super(device);
        DeviceType type = device.getType();
    }
}
