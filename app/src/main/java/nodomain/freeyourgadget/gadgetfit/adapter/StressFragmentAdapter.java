package nodomain.freeyourgadget.gadgetfit.adapter;

import androidx.fragment.app.Fragment;

import nodomain.freeyourgadget.gadgetfit.activities.charts.StressDailyFragment;
import nodomain.freeyourgadget.gadgetfit.activities.charts.StressPeriodFragment;

public class StressFragmentAdapter extends NestedFragmentAdapter {
    public StressFragmentAdapter(Fragment fragment) {
        super(fragment);
    }

    @Override
    public Fragment createFragment(int position) {
        switch (position) {
            case 0:
                return new StressDailyFragment();
            case 1:
                return StressPeriodFragment.newInstance(7);
            case 2:
                return StressPeriodFragment.newInstance(30);
        }
        return new StressDailyFragment();
    }
}