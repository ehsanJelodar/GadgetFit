package nodomain.freeyourgadget.gadgetfit.service.devices.huawei.requests;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSettingsPreferenceConst;
import nodomain.freeyourgadget.gadgetfit.devices.huawei.HuaweiPacket;
import nodomain.freeyourgadget.gadgetfit.devices.huawei.packets.Earphones;
import nodomain.freeyourgadget.gadgetfit.service.devices.huawei.HuaweiSupportProvider;

public class SetPauseWhenRemovedFromEarRequest extends Request {

    public SetPauseWhenRemovedFromEarRequest(HuaweiSupportProvider supportProvider) {
        super(supportProvider);
        this.serviceId = Earphones.id;
        this.commandId = Earphones.SetAudioModeRequest.id;
        this.addToResponse = false; // Response with different command ID
    }

    @Override
    protected List<byte[]> createRequest() throws RequestCreationException {
        try {
            boolean newState = GBApplication
                    .getDeviceSpecificSharedPrefs(this.getDevice().getAddress())
                    .getBoolean(DeviceSettingsPreferenceConst.PREF_HUAWEI_FREEBUDS_INEAR, false);
            return new Earphones.SetPauseWhenRemovedFromEar(this.paramsProvider, newState).serialize();
        } catch (HuaweiPacket.CryptoException e) {
            throw new RequestCreationException(e);
        }
    }
}
