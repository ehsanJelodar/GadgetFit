package nodomain.freeyourgadget.gadgetfit.service.devices.garmin.deviceevents;

import android.content.Context;

import java.util.Set;

import nodomain.freeyourgadget.gadgetfit.deviceevents.GBDeviceEvent;
import nodomain.freeyourgadget.gadgetfit.devices.garmin.GarminCapability;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class CapabilitiesDeviceEvent extends GBDeviceEvent {
    public Set<GarminCapability> capabilities;

    public CapabilitiesDeviceEvent(final Set<GarminCapability> capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public void evaluate(final Context context, final GBDevice device) {
        // Handled in support class
    }
}
