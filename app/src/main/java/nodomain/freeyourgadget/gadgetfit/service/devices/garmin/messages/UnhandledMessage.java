package nodomain.freeyourgadget.gadgetfit.service.devices.garmin.messages;

import nodomain.freeyourgadget.gadgetfit.service.devices.garmin.messages.status.GenericStatusMessage;

public class UnhandledMessage extends GFDIMessage {

    private final int messageType;

    public UnhandledMessage(int messageType) {
        this.messageType = messageType;

        this.statusMessage = new GenericStatusMessage(messageType, Status.UNSUPPORTED);

    }

    @Override
    protected boolean generateOutgoing() {
        return false;
    }
}
