/*  Copyright (C) 2020-2024 Andreas Shimokawa, Vianney le Clément de
    Saint-Marcq

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
package nodomain.freeyourgadget.gadgetfit.service.devices.huami.amazfitgts2;

import android.content.Context;
import android.net.Uri;

import java.io.IOException;

import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.devices.huami.HuamiFWHelper;
import nodomain.freeyourgadget.gadgetfit.devices.huami.amazfitgts2.AmazfitGTS2FWHelper;
import nodomain.freeyourgadget.gadgetfit.model.CallSpec;
import nodomain.freeyourgadget.gadgetfit.service.btle.TransactionBuilder;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.amazfitgts.AmazfitGTSSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.operations.update.UpdateFirmwareOperation;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.operations.update.UpdateFirmwareOperation2020;

public class AmazfitGTS2Support extends AmazfitGTSSupport {

    @Override
    public boolean supportsSunriseSunsetWindHumidity() {
        return true;
    }

    @Override
    public HuamiFWHelper createFWHelper(Uri uri, Context context) throws IOException {
        return new AmazfitGTS2FWHelper(uri, context);
    }

    @Override
    public void onSetCallState(CallSpec callSpec) {
        onSetCallStateNew(callSpec);
    }

    @Override
    public UpdateFirmwareOperation createUpdateFirmwareOperation(Uri uri) {
        return new UpdateFirmwareOperation2020(uri, this);
    }

    @Override
    public int getActivitySampleSize() {
        return 8;
    }

    @Override
    protected AmazfitGTS2Support setDisplayItems(TransactionBuilder builder) {
        setDisplayItemsNew(builder, false, false, R.array.pref_gtsgtr2_display_items_default);
        return this;
    }
}
