/*  Copyright (C) 2023-2024 José Rebelo

    This file is part of gadgetbridge.

    gadgetbridge is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    gadgetbridge is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>. */
package nodomain.freeyourgadget.gadgetfit.service.devices.huami.amazfitbip3pro;

import android.content.Context;
import android.net.Uri;

import java.io.IOException;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.huami.HuamiFWHelper;
import nodomain.freeyourgadget.gadgetfit.devices.huami.amazfitbip3pro.AmazfitBip3ProFWHelper;
import nodomain.freeyourgadget.gadgetfit.model.CallSpec;
import nodomain.freeyourgadget.gadgetfit.model.NotificationSpec;
import nodomain.freeyourgadget.gadgetfit.service.btle.TransactionBuilder;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.amazfitbip.AmazfitBipSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.operations.update.UpdateFirmwareOperation;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.operations.update.UpdateFirmwareOperation2020;

public class AmazfitBip3ProSupport extends AmazfitBipSupport {
    @Override
    public byte getCryptFlags() {
        return (byte) 0x80;
    }

    @Override
    protected byte getAuthFlags() {
        return 0x00;
    }

    @Override
    protected boolean notificationHasExtraHeader() {
        return true;
    }

    @Override
    public boolean supportsSunriseSunsetWindHumidity() {
        return true;
    }

    @Override
    public HuamiFWHelper createFWHelper(final Uri uri, final Context context) throws IOException {
        return new AmazfitBip3ProFWHelper(uri, context);
    }

    @Override
    public UpdateFirmwareOperation createUpdateFirmwareOperation(final Uri uri) {
        return new UpdateFirmwareOperation2020(uri, this);
    }

    @Override
    public void onSetCallState(final CallSpec callSpec) {
        onSetCallStateNew(callSpec);
    }

    @Override
    public int getActivitySampleSize() {
        return 8;
    }

    @Override
    protected AmazfitBip3ProSupport setDisplayItems(final TransactionBuilder builder) {
        setDisplayItemsNew(builder, false, false, R.array.pref_gtsgtr2_display_items_default);
        return this;
    }

    @Override
    public String getNotificationBody(NotificationSpec notificationSpec) {
        // See #4419
        return getNotificationBodyCheckAcceptsSender(notificationSpec);
    }
}
