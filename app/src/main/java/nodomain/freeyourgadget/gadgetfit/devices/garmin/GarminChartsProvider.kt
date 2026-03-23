package nodomain.freeyourgadget.gadgetfit.devices.garmin

import android.content.Context
import nodomain.freeyourgadget.gadgetfit.R
import nodomain.freeyourgadget.gadgetfit.activities.charts.DefaultChartsProvider
import nodomain.freeyourgadget.gadgetfit.activities.workouts.entries.ActivitySummarySimpleEntry
import nodomain.freeyourgadget.gadgetfit.database.DBHandler
import nodomain.freeyourgadget.gadgetfit.devices.GarminSleepRestlessMomentsSampleProvider
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice
import nodomain.freeyourgadget.gadgetfit.model.ActivitySummaryEntries.UNIT_NONE

class GarminChartsProvider : DefaultChartsProvider() {
    override fun getDailySleepStats(
        context: Context,
        db: DBHandler,
        device: GBDevice,
        tsStart: Int,
        tsEnd: Int
    ): Map<String, ActivitySummarySimpleEntry> {
        val sleepRestlessMomentsSampleProvider = GarminSleepRestlessMomentsSampleProvider(device, db.getDaoSession())
        val restlessMoments = sleepRestlessMomentsSampleProvider.getAllSamples(tsStart * 1000L, tsEnd * 1000L)
        if (restlessMoments.isEmpty()) {
            return emptyMap()
        }
        restlessMoments.sumOf { it.count }
        return mapOf(
            context.getString(R.string.sleep_restless_moments) to ActivitySummarySimpleEntry(
                restlessMoments.sumOf { it.count },
                UNIT_NONE
            )
        )
    }
}
