package nodomain.freeyourgadget.gadgetfit.activities.charts

import android.content.Context
import androidx.fragment.app.Fragment
import nodomain.freeyourgadget.gadgetfit.activities.workouts.entries.ActivitySummarySimpleEntry
import nodomain.freeyourgadget.gadgetfit.database.DBHandler
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice

interface DeviceChartsProvider {
    fun getSupportedCharts(device: GBDevice): List<String>

    fun getEnabledCharts(device: GBDevice): List<String>

    fun getChartLabel(context: Context, device: GBDevice, chartName: String): String

    fun getChartFragment(device: GBDevice, chartName: String, allowSwipe: Boolean): Fragment

    fun getDailySleepStats(
        context: Context,
        db: DBHandler,
        device: GBDevice,
        tsStart: Int,
        tsEnd: Int
    ): Map<String, ActivitySummarySimpleEntry>
}
