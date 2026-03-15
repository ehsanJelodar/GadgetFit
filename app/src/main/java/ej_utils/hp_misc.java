package ej_utils;

import android.content.Context;
import android.graphics.drawable.Drawable;
import android.os.Build;
import android.os.PowerManager;
import android.view.View;

public class hp_misc {

    public static boolean isScreenOn(Context context)
    {
        PowerManager powerManager = (PowerManager) context.getSystemService(Context.POWER_SERVICE);
        if (powerManager == null) {
            return false;
        }
        return powerManager.isInteractive();
    }
}
