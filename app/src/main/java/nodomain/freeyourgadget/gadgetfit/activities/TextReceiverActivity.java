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
package nodomain.freeyourgadget.gadgetfit.activities;

import android.content.Intent;
import android.os.Bundle;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nodomain.freeyourgadget.gadgetfit.BuildConfig;
import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.model.NotificationSpec;
import nodomain.freeyourgadget.gadgetfit.model.NotificationType;

/**
 * Receive any shared plaintext and forward it directly to the devices as a notification.
 */
public class TextReceiverActivity extends AbstractGBActivity {
    private static final Logger LOG = LoggerFactory.getLogger(TextReceiverActivity.class);

    @Override
    protected void onCreate(final Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        final Intent intent = getIntent();

        final String action = intent.getAction();
        if (!Intent.ACTION_SEND.equals(action)) {
            LOG.warn("Unknown action '{}'", action);
            finish();
            return;
        }

        final String type = intent.getType();
        if (!"text/plain".equals(type)) {
            LOG.warn("Unknown type '{}'", type);
            finish();
            return;
        }

        final String text = intent.getStringExtra(Intent.EXTRA_TEXT);
        if (StringUtils.isBlank(text)) {
            LOG.warn("Text is null or empty");
            finish();
            return;
        }

        LOG.info("Sending '{}' to all devices", text);

        final NotificationSpec notificationSpec = new NotificationSpec();
        final String appName = getApplicationContext().getApplicationInfo()
                .loadLabel(getApplicationContext().getPackageManager())
                .toString();
        notificationSpec.title = appName;
        notificationSpec.body = text;
        notificationSpec.sourceAppId = BuildConfig.APPLICATION_ID;
        notificationSpec.sourceName = appName;
        notificationSpec.type = NotificationType.gadgetfit_TEXT_RECEIVER;

        GBApplication.deviceService().onNotification(notificationSpec);
        finish();
    }
}
