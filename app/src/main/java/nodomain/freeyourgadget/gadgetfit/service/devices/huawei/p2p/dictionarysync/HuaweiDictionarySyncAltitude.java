package nodomain.freeyourgadget.gadgetfit.service.devices.huawei.p2p.dictionarysync;

import android.content.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.devices.huawei.HuaweiState;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.devices.huawei.p2p.HuaweiP2PDataDictionarySyncService;

//TODO: not fully discovered.
public class HuaweiDictionarySyncAltitude implements  HuaweiDictionarySyncInterface {
    private final Logger LOG = LoggerFactory.getLogger(HuaweiDictionarySyncAltitude.class);

    public static final int ALTITUDE_CLASS = 200003;

    @Override
    public int getDataClass() {
        return ALTITUDE_CLASS;
    }

    @Override
    public boolean supports(HuaweiState state) {
        return false; // state.supportsAltitude()
    }

    @Override
    public long getLastDataSyncTimestamp(GBDevice gbDevice) {
        return 0;
    }

    @Override
    public void handleData(Context context, GBDevice gbDevice, List<HuaweiP2PDataDictionarySyncService.DictData> dictData) {

    }
}
