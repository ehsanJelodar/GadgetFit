package nodomain.freeyourgadget.gadgetfit.service.devices.garmin.deviceevents;

import android.content.Context;

import nodomain.freeyourgadget.gadgetfit.deviceevents.GBDeviceEvent;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class NotificationSubscriptionDeviceEvent extends GBDeviceEvent {
    public boolean enable;

    @Override
    public void evaluate(final Context context, final GBDevice device) {
        // Handled in support class
    }
}
