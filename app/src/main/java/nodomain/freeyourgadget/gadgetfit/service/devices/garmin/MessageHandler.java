package nodomain.freeyourgadget.gadgetfit.service.devices.garmin;

import nodomain.freeyourgadget.gadgetfit.service.devices.garmin.messages.GFDIMessage;

public interface MessageHandler {
    GFDIMessage handle(GFDIMessage message);
}
