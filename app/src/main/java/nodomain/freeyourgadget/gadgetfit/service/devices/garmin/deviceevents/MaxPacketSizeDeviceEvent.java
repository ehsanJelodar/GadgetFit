package nodomain.freeyourgadget.gadgetfit.service.devices.garmin.deviceevents;

import android.content.Context;

import nodomain.freeyourgadget.gadgetfit.deviceevents.GBDeviceEvent;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class MaxPacketSizeDeviceEvent extends GBDeviceEvent {
    private final int maxPacketSize;

    public MaxPacketSizeDeviceEvent(final int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    @Override
    public void evaluate(final Context context, final GBDevice device) {
        // Handled in support class
    }
}
