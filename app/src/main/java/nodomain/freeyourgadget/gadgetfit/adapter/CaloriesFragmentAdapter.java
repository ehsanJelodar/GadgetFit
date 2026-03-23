package nodomain.freeyourgadget.gadgetfit.adapter;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;

import nodomain.freeyourgadget.gadgetfit.activities.charts.CaloriesDailyFragment;
import nodomain.freeyourgadget.gadgetfit.activities.charts.CaloriesPeriodFragment;

public class CaloriesFragmentAdapter extends NestedFragmentAdapter {

    public CaloriesFragmentAdapter(Fragment fragment) {
        super(fragment);
    }

    @NonNull
    @Override
    public Fragment createFragment(int position) {
        switch (position) {
            case 0:
                return new CaloriesDailyFragment();
            case 1:
                return CaloriesPeriodFragment.newInstance(7);
            case 2:
                return CaloriesPeriodFragment.newInstance(30);
        }
        return new CaloriesDailyFragment();
    }
}
