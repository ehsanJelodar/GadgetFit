package nodomain.freeyourgadget.gadgetfit.service.devices.huawei.p2p.dictionarysync;

import android.content.Context;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.devices.huawei.HuaweiState;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.service.devices.huawei.p2p.HuaweiP2PDataDictionarySyncService;

public interface HuaweiDictionarySyncInterface {

    int getDataClass();

    boolean supports(HuaweiState state);

    long getLastDataSyncTimestamp(GBDevice gbDevice);

    void handleData(Context context, GBDevice gbDevice, List<HuaweiP2PDataDictionarySyncService.DictData> dictData);
}
