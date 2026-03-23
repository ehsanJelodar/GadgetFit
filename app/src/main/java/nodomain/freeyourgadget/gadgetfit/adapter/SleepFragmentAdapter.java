package nodomain.freeyourgadget.gadgetfit.adapter;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;


import nodomain.freeyourgadget.gadgetfit.activities.charts.SleepDailyFragment;
import nodomain.freeyourgadget.gadgetfit.activities.charts.SleepPeriodFragment;

public class SleepFragmentAdapter extends NestedFragmentAdapter {
    public SleepFragmentAdapter(Fragment fragment) {
        super(fragment);
    }

    @NonNull
    @Override
    public Fragment createFragment(int position) {
        switch (position) {
            case 0:
                return new SleepDailyFragment();
            case 1:
                return SleepPeriodFragment.newInstance(7);
            case 2:
                return SleepPeriodFragment.newInstance(30);
        }
        return new SleepDailyFragment();
    }
}
