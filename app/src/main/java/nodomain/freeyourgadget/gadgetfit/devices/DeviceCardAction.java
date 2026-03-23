package nodomain.freeyourgadget.gadgetfit.devices;

import android.content.Context;

import androidx.annotation.DrawableRes;
import androidx.annotation.Nullable;

import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public interface DeviceCardAction {
    @DrawableRes
    int getIcon(final GBDevice device);

    String getDescription(final GBDevice device, final Context context);

    @Nullable
    default String getLabel(final GBDevice device, final Context context) {
        return null;
    }

    default boolean isVisible(final GBDevice device) {
        return device.isConnected();
    }

    void onClick(final GBDevice device, final Context context);
}
