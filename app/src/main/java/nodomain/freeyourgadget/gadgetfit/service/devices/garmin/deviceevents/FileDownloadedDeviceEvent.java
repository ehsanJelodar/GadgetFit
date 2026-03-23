package nodomain.freeyourgadget.gadgetfit.service.devices.garmin.deviceevents;

import android.content.Context;

import nodomain.freeyourgadget.gadgetfit.deviceevents.GBDeviceEvent;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.devices.garmin.FileTransferHandler;

public class FileDownloadedDeviceEvent extends GBDeviceEvent {
    public boolean success = true;
    public FileTransferHandler.DirectoryEntry directoryEntry;
    public String localPath;

    @Override
    public void evaluate(final Context context, final GBDevice device) {
        // Handled in support class
    }
}
