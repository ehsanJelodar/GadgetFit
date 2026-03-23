/*  Copyright (C) 2015-2024 Andreas Shimokawa, Arjan Schrijver, Carsten
    Pfeiffer

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
package nodomain.freeyourgadget.gadgetfit.adapter;

import android.content.Context;
import android.graphics.Bitmap;

import java.util.List;

import nodomain.freeyourgadget.gadgetfit.model.ItemWithDetails;

/**
 * Adapter for displaying generic ItemWithDetails instances.
 */
public class ItemWithDetailsAdapter extends AbstractItemAdapter<ItemWithDetails> {

    public ItemWithDetailsAdapter(Context context, List<ItemWithDetails> items) {
        super(context, items);
    }

    @Override
    protected String getName(ItemWithDetails item) {
        return item.getName();
    }

    @Override
    protected String getDetails(ItemWithDetails item) {
        return item.getDetails();
    }

    @Override
    protected int getIcon(ItemWithDetails item) {
        return item.getIcon();
    }

    @Override
    protected Bitmap getPreview(ItemWithDetails item) {
        return item.getPreview();
    }
}
