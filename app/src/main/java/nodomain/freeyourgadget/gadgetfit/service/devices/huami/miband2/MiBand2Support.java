/*  Copyright (C) 2018-2024 Andreas Shimokawa, José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.service.devices.huami.miband2;

import static nodomain.freeyourgadget.gadgetfit.service.btle.GattCharacteristic.UUID_CHARACTERISTIC_ALERT_LEVEL;

import android.content.Context;
import android.net.Uri;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import nodomain.freeyourgadget.gadgetfit.devices.huami.HuamiFWHelper;
import nodomain.freeyourgadget.gadgetfit.devices.huami.HuamiService;
import nodomain.freeyourgadget.gadgetfit.devices.huami.miband2.MiBand2FWHelper;
import nodomain.freeyourgadget.gadgetfit.devices.miband.MiBandConst;
import nodomain.freeyourgadget.gadgetfit.model.NotificationSpec;
import nodomain.freeyourgadget.gadgetfit.model.NotificationType;
import nodomain.freeyourgadget.gadgetfit.service.btle.BLETypeConversions;
import nodomain.freeyourgadget.gadgetfit.service.btle.profiles.alertnotification.AlertCategory;
import nodomain.freeyourgadget.gadgetfit.service.devices.common.SimpleNotification;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.HuamiSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.actions.StopNotificationAction;
import nodomain.freeyourgadget.gadgetfit.util.NotificationUtils;

public class MiBand2Support extends HuamiSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MiBand2Support.class);

    private boolean alarmClockRinging;

    @Override
    public HuamiFWHelper createFWHelper(Uri uri, Context context) throws IOException {
        return new MiBand2FWHelper(uri, context);
    }

    @Override
    public void onNotification(NotificationSpec notificationSpec) {
        if (notificationSpec.type == NotificationType.GENERIC_ALARM_CLOCK) {
            onAlarmClock(notificationSpec);
            return;
        }

        int alertLevel = HuamiService.ALERT_LEVEL_MESSAGE;
        if (notificationSpec.type == NotificationType.UNKNOWN) {
            alertLevel = HuamiService.ALERT_LEVEL_VIBRATE_ONLY;
        }
        String message = NotificationUtils.getPreferredTextFor(notificationSpec, 40, 40, getContext()).trim();
        String origin = notificationSpec.type.getGenericType();
        SimpleNotification simpleNotification = new SimpleNotification(message, BLETypeConversions.toAlertCategory(notificationSpec.type), notificationSpec.type);
        performPreferredNotification(origin + " received", origin, simpleNotification, alertLevel, null);
    }

    protected void onAlarmClock(NotificationSpec notificationSpec) {
        alarmClockRinging = true;
        StopNotificationAction abortAction = new StopNotificationAction(getCharacteristic(UUID_CHARACTERISTIC_ALERT_LEVEL)) {
            @Override
            protected boolean shouldAbort() {
                return !isAlarmClockRinging();
            }
        };
        String message = NotificationUtils.getPreferredTextFor(notificationSpec, 40, 40, getContext());
        SimpleNotification simpleNotification = new SimpleNotification(message, AlertCategory.HighPriorityAlert, notificationSpec.type);
        performPreferredNotification("alarm clock ringing", MiBandConst.ORIGIN_ALARM_CLOCK, simpleNotification, HuamiService.ALERT_LEVEL_VIBRATE_ONLY, abortAction);
    }

    @Override
    public void onDeleteNotification(int id) {
        alarmClockRinging = false; // we should have the notificationtype at least to check
    }

    private boolean isAlarmClockRinging() {
        // don't synchronize, this is not really important
        return alarmClockRinging;
    }

}
