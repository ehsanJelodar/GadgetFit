package nodomain.freeyourgadget.gadgetfit.model.workout

import nodomain.freeyourgadget.gadgetfit.entities.BaseActivitySummary
import nodomain.freeyourgadget.gadgetfit.model.ActivitySummaryData

data class Workout @JvmOverloads constructor(
    val summary: BaseActivitySummary,
    val data: ActivitySummaryData,
    val charts: List<WorkoutChart> = emptyList()
)
