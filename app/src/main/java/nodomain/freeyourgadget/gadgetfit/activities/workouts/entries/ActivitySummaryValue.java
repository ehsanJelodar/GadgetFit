package nodomain.freeyourgadget.gadgetfit.activities.workouts.entries;

import nodomain.freeyourgadget.gadgetfit.activities.workouts.WorkoutValueFormatter;
import nodomain.freeyourgadget.gadgetfit.model.ActivitySummaryEntries;

public record ActivitySummaryValue(Object value, String unit) {
    public ActivitySummaryValue(final String value) {
        this(value, ActivitySummaryEntries.UNIT_STRING);
    }

    public String format(final WorkoutValueFormatter formatter) {
        return formatter.formatValue(value, unit);
    }
}
