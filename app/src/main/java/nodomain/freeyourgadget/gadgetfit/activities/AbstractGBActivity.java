/*  Copyright (C) 2017-2024 Andreas Shimokawa, Arjan Schrijver, Carsten
    Pfeiffer, Daniele Gobbetti, Petr Vaněk

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
package nodomain.freeyourgadget.gadgetfit.activities;


import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;
import androidx.localbroadcastmanager.content.LocalBroadcastManager;

import java.util.Locale;

import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.util.AndroidUtils;


public abstract class AbstractGBActivity extends AppCompatActivity implements GBActivity {
    private boolean isLanguageInvalid = false;

    public static final int NONE = 0;
    public static final int NO_ACTIONBAR = 1;

    private final BroadcastReceiver mReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (action == null) {
                return;
            }
            switch (action) {
                case GBApplication.ACTION_LANGUAGE_CHANGE:
                    setLanguage(GBApplication.getLanguage(), true);
                    break;
                case GBApplication.ACTION_THEME_CHANGE:
                    recreate();
                    break;
                case GBApplication.ACTION_QUIT:
                    finish();
                    break;
            }
        }
    };

    public void setLanguage(Locale language, boolean invalidateLanguage) {
        if (invalidateLanguage) {
            isLanguageInvalid = true;
        }
        AndroidUtils.setLanguage(this, language);
    }

    public static void init(GBActivity activity) {
        init(activity, NONE);
    }

    public static void init(GBActivity activity, int flags) {
        if (GBApplication.areDynamicColorsEnabled()) {
            if (GBApplication.isDarkThemeEnabled()) {
                if ((flags & NO_ACTIONBAR) != 0) {
                    if (GBApplication.isAmoledBlackEnabled())
                        activity.setTheme(R.style.gadgetfitThemeDynamicDarkAmoled_NoActionBar);
                    else
                        activity.setTheme(R.style.gadgetfitThemeDynamicDark_NoActionBar);
                } else {
                    if (GBApplication.isAmoledBlackEnabled())
                        activity.setTheme(R.style.gadgetfitThemeDynamicDarkAmoled);
                    else
                        activity.setTheme(R.style.gadgetfitThemeDynamicDark);
                }
            } else {
                if ((flags & NO_ACTIONBAR) != 0) {
                    activity.setTheme(R.style.gadgetfitThemeDynamicLight_NoActionBar);
                } else {
                    activity.setTheme(R.style.gadgetfitThemeDynamicLight);
                }
            }
        } else if (GBApplication.isDarkThemeEnabled()) {
            if ((flags & NO_ACTIONBAR) != 0) {
                if (GBApplication.isAmoledBlackEnabled())
                    activity.setTheme(R.style.gadgetfitThemeBlack_NoActionBar);
                else
                    activity.setTheme(R.style.gadgetfitThemeDark_NoActionBar);
            } else {
                if (GBApplication.isAmoledBlackEnabled())
                    activity.setTheme(R.style.gadgetfitThemeBlack);
                else
                    activity.setTheme(R.style.gadgetfitThemeDark);
            }
        } else {
            if ((flags & NO_ACTIONBAR) != 0) {
                activity.setTheme(R.style.gadgetfitTheme_NoActionBar);
            } else {
                activity.setTheme(R.style.gadgetfitTheme);
            }
        }
        activity.setLanguage(GBApplication.getLanguage(), false);
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        IntentFilter filterLocal = new IntentFilter();
        filterLocal.addAction(GBApplication.ACTION_QUIT);
        filterLocal.addAction(GBApplication.ACTION_LANGUAGE_CHANGE);
        filterLocal.addAction(GBApplication.ACTION_THEME_CHANGE);
        LocalBroadcastManager.getInstance(this).registerReceiver(mReceiver, filterLocal);

        init(this);
        super.onCreate(savedInstanceState);
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (isLanguageInvalid) {
            isLanguageInvalid = false;
            recreate();
        }
    }

    @Override
    protected void onDestroy() {
        LocalBroadcastManager.getInstance(this).unregisterReceiver(mReceiver);
        super.onDestroy();
    }
}
