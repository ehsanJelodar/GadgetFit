package nodomain.freeyourgadget.gadgetfit.activities.workouts.charts

import nodomain.freeyourgadget.gadgetfit.model.workout.WorkoutChart

object ChartDataRepository {
    var chartData: List<WorkoutChart>? = null

    fun clear() {
        chartData = null
    }
}