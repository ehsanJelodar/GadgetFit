package nodomain.freeyourgadget.gadgetfit.service.devices.garmin.deviceevents;

import android.content.Context;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.deviceevents.GBDeviceEvent;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.devices.garmin.fit.RecordDefinition;


public class IncomingFitDefinitionDeviceEvent extends GBDeviceEvent {
    public List<RecordDefinition> getRecordDefinitions() {
        return recordDefinitions;
    }

    private final List<RecordDefinition> recordDefinitions;

    public IncomingFitDefinitionDeviceEvent(List<RecordDefinition> recordDefinitions) {
        this.recordDefinitions = recordDefinitions;
    }

    @Override
    public void evaluate(final Context context, final GBDevice device) {
        // Handled in support class
    }
}
