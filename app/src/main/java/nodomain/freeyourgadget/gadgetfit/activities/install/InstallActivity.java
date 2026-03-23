/*  Copyright (C) 2015-2024 Andreas Shimokawa, Carsten Pfeiffer, José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.activities.install;

import android.graphics.Bitmap;

import androidx.annotation.Nullable;

import nodomain.freeyourgadget.gadgetfit.model.ItemWithDetails;

public interface InstallActivity {
    CharSequence getInfoText();

    void setInfoText(String text);

    void setPreview(@Nullable Bitmap bitmap);

    void setInstallEnabled(boolean enable);

    void setCloseEnabled(boolean enable);

    void clearInstallItems();

    void setInstallItem(ItemWithDetails item);

}
