package nodomain.freeyourgadget.gadgetfit.service.devices.soundcore.liberty;

import java.util.UUID;

import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.btbr.TransactionBuilder;
import nodomain.freeyourgadget.gadgetfit.service.serial.AbstractHeadphoneSerialDeviceSupportV2;

public class SoundcoreLiberty4NCDeviceSupport extends AbstractHeadphoneSerialDeviceSupportV2<SoundcoreLibertyProtocol> {
    public static final UUID UUID_DEVICE_CTRL = UUID.fromString("0cf12d31-fac3-4553-bd80-d6832e7b3947");

    public SoundcoreLiberty4NCDeviceSupport() {
        addSupportedService(UUID_DEVICE_CTRL);
    }

    @Override
    protected SoundcoreLibertyProtocol createDeviceProtocol() {
        return new SoundcoreLibertyProtocol(getDevice());
    }

    @Override
    protected TransactionBuilder initializeDevice(final TransactionBuilder builder) {
        builder.write(mDeviceProtocol.encodeDeviceInfoRequest());
        builder.setDeviceState(GBDevice.State.INITIALIZED);
        return builder;
    }
}
