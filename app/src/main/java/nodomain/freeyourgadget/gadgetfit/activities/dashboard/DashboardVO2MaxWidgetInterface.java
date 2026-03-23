package nodomain.freeyourgadget.gadgetfit.activities.dashboard;

import nodomain.freeyourgadget.gadgetfit.model.Vo2MaxSample;

public interface DashboardVO2MaxWidgetInterface {
    Vo2MaxSample.Type getVO2MaxType();
    String getWidgetKey();
}
