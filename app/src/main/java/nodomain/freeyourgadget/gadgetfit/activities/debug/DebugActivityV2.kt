package nodomain.freeyourgadget.gadgetfit.activities.debug

import androidx.preference.PreferenceFragmentCompat
import nodomain.freeyourgadget.gadgetfit.activities.AbstractSettingsActivityV2

class DebugActivityV2 : AbstractSettingsActivityV2(), PreferenceFragmentCompat.OnPreferenceStartFragmentCallback {
    override fun newFragment(): PreferenceFragmentCompat? {
        return MainDebugFragment()
    }
}
