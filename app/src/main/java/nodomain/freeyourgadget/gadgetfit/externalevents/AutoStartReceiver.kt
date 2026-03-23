/*  Copyright (C) 2017-2024 Carsten Pfeiffer, Daniele Gobbetti, Felix
    Konstantin Maurer, Petr Vaněk

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
package nodomain.freeyourgadget.gadgetfit.externalevents

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log
import nodomain.freeyourgadget.gadgetfit.GBApplication
import nodomain.freeyourgadget.gadgetfit.database.PeriodicDbExporter
import nodomain.freeyourgadget.gadgetfit.util.GBPrefs
import nodomain.freeyourgadget.gadgetfit.util.backup.PeriodicZipExporter

class AutoStartReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (!GBApplication.getPrefs().autoStart) {
            return
        }
        if (Intent.ACTION_BOOT_COMPLETED != intent.action && Intent.ACTION_MY_PACKAGE_REPLACED != intent.action) {
            return
        }

        Log.i(TAG, "Boot or reinstall completed, starting gadgetfit")

        if (GBApplication.getPrefs().getBoolean(GBPrefs.AUTO_CONNECT_BLUETOOTH, false)) {
            Log.i(TAG, "Auto-connect is enabled, attempting to connect")
            GBApplication.deviceService().connect()
        }

        Log.i(TAG, "Going to enable periodic db exporter")
        PeriodicDbExporter.scheduleNextExecution(context)

        Log.i(TAG, "Going to enable periodic zip exporter")
        PeriodicZipExporter.scheduleNextExecution(context)
    }

    companion object {
        private val TAG: String = AutoStartReceiver::class.java.getName()
    }
}
