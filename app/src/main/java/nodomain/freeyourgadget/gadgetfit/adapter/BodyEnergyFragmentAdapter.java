package nodomain.freeyourgadget.gadgetfit.adapter;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;

import nodomain.freeyourgadget.gadgetfit.activities.charts.BodyEnergyFragment;
import nodomain.freeyourgadget.gadgetfit.activities.charts.BodyEnergyPeriodFragment;

public class BodyEnergyFragmentAdapter extends NestedFragmentAdapter {

    public BodyEnergyFragmentAdapter(Fragment fragment) {
        super(fragment);
    }

    @NonNull
    @Override
    public Fragment createFragment(int position) {
        switch (position) {
            case 0:
                return new BodyEnergyFragment();
            case 1:
                return BodyEnergyPeriodFragment.newInstance(7);
            case 2:
                return BodyEnergyPeriodFragment.newInstance(30);
        }
        return new BodyEnergyFragment();
    }
}

