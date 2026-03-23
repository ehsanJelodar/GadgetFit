/*  Copyright (C) 2025  Thomas Kuehne

    This file is part of gadgetbridge.

    gadgetbridge is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    gadgetbridge is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>. */

package nodomain.freeyourgadget.gadgetfit.devices.ultrahuman;

import android.content.SharedPreferences;

import androidx.annotation.DrawableRes;
import androidx.annotation.NonNull;
import androidx.annotation.StringRes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSettingsPreferenceConst;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettings;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.ComputedHrvSummarySampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCardAction;
import nodomain.freeyourgadget.gadgetfit.devices.GenericHeartRateSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.GenericHrvValueSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.GenericSpo2SampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.GenericStressSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.GenericTemperatureSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.SampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.ultrahuman.samples.UltrahumanActivitySampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.GenericHeartRateSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GenericHrvValueSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GenericSpo2SampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GenericStressSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GenericTemperatureSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.UltrahumanActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.UltrahumanDeviceStateSampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.impl.GBDeviceCandidate;
import nodomain.freeyourgadget.gadgetfit.model.ActivitySample;
import nodomain.freeyourgadget.gadgetfit.model.DeviceType;
import nodomain.freeyourgadget.gadgetfit.model.HeartRateSample;
import nodomain.freeyourgadget.gadgetfit.model.HrvSummarySample;
import nodomain.freeyourgadget.gadgetfit.model.HrvValueSample;
import nodomain.freeyourgadget.gadgetfit.model.Spo2Sample;
import nodomain.freeyourgadget.gadgetfit.model.StressSample;
import nodomain.freeyourgadget.gadgetfit.model.TemperatureSample;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.ultrahuman.UltrahumanDeviceSupport;
import nodomain.freeyourgadget.gadgetfit.util.GBPrefs;
import nodomain.freeyourgadget.gadgetfit.util.preferences.DevicePrefs;

public class UltrahumanDeviceCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    public GBDevice createDevice(GBDeviceCandidate candidate, DeviceType deviceType) {
        GBDevice gbDevice = super.createDevice(candidate, deviceType);

        DevicePrefs devicePreferences = GBApplication.getDevicePrefs(gbDevice);
        SharedPreferences preferences = devicePreferences.getPreferences();
        SharedPreferences.Editor editor = preferences.edit();

        // a low powered BLE gadget with gadget initiated connections
        editor.putBoolean(DeviceSettingsPreferenceConst.PREF_CONNECTION_PRIORITY_LOW_POWER, true);
        editor.putBoolean(GBPrefs.DEVICE_CONNECT_BACK, true);
        editor.putBoolean(GBPrefs.DEVICE_AUTO_RECONNECT, true);

        // the gadget loses it's clock when the battery is low
        editor.putBoolean(DeviceSettingsPreferenceConst.PREF_TIME_SYNC, true);

        // O2 measurement with smart rings is still work in progress
        editor.putBoolean(DeviceSettingsPreferenceConst.PREF_SPO2_ALL_DAY_MONITORING, false);

        editor.apply();

        return gbDevice;
    }

    @NonNull
    @Override
    public List<DeviceCardAction> getCustomActions() {
        List<DeviceCardAction> list = new ArrayList<>(2);
        list.add(new UltrahumanDeviceCardAction(R.drawable.ic_flight, R.string.ultrahuman_airplane_mode_title, R.string.ultrahuman_airplane_mode_question, UltrahumanConstants.ACTION_AIRPLANE_MODE));
        list.add(new UltrahumanDeviceCardAction(R.drawable.ic_pulmonology, R.string.ultrahuman_breathing_title, UltrahumanBreathingActivity.class));
        return list;
    }

    @NonNull
    @Override
    public Map<AbstractDao<?, ?>, Property> getAllDeviceDao(@NonNull final DaoSession session) {
        Map<AbstractDao<?, ?>, Property> map = new HashMap<>(7);
        map.put(session.getGenericHeartRateSampleDao(), GenericHeartRateSampleDao.Properties.DeviceId);
        map.put(session.getGenericHrvValueSampleDao(), GenericHrvValueSampleDao.Properties.DeviceId);
        map.put(session.getGenericSpo2SampleDao(), GenericSpo2SampleDao.Properties.DeviceId);
        map.put(session.getGenericStressSampleDao(), GenericStressSampleDao.Properties.DeviceId);
        map.put(session.getGenericTemperatureSampleDao(), GenericTemperatureSampleDao.Properties.DeviceId);
        map.put(session.getUltrahumanActivitySampleDao(), UltrahumanActivitySampleDao.Properties.DeviceId);
        map.put(session.getUltrahumanDeviceStateSampleDao(), UltrahumanDeviceStateSampleDao.Properties.DeviceId);
        return map;
    }

    @DrawableRes
    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_smartring;
    }

    @StringRes
    @Override
    public int getDeviceNameResource() {
        return R.string.devicetype_ultrahuma_ring_air;
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return UltrahumanDeviceSupport.class;
    }

    @Override
    public DeviceSpecificSettings getDeviceSpecificSettings(GBDevice device) {
        final DeviceSpecificSettings settings = new DeviceSpecificSettings();
        settings.addRootScreen(R.xml.devicesettings_ultrahuman_air);
        return settings;
    }

    @Override
    public String getManufacturer() {
        return "Ultrahuman";
    }

    @Override
    public TimeSampleProvider<? extends HrvValueSample> getHrvValueSampleProvider(GBDevice device, DaoSession session) {
        return new GenericHrvValueSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends HrvSummarySample> getHrvSummarySampleProvider(GBDevice device, DaoSession session) {
        return new ComputedHrvSummarySampleProvider(getHrvValueSampleProvider(device, session), device, session);
    }

    @Override
    public TimeSampleProvider<? extends HeartRateSample> getHeartRateMaxSampleProvider(GBDevice device, DaoSession session) {
        return new GenericHeartRateSampleProvider(device, session);
    }

    @Override
    public SampleProvider<? extends ActivitySample> getSampleProvider(final GBDevice device, final DaoSession session) {
        return new UltrahumanActivitySampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends Spo2Sample> getSpo2SampleProvider(GBDevice device, DaoSession session) {
        return new GenericSpo2SampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends StressSample> getStressSampleProvider(GBDevice device, DaoSession session) {
        return new GenericStressSampleProvider(device, session);
    }

    @Override
    protected Pattern getSupportedDeviceName() {
        return Pattern.compile("^UH_[0-9A-F]{12}$");
    }

    @Override
    public TimeSampleProvider<? extends TemperatureSample> getTemperatureSampleProvider(GBDevice device, DaoSession session) {
        return new GenericTemperatureSampleProvider(device, session, TemperatureSample.TYPE_SKIN, TemperatureSample.LOCATION_FINGER);
    }

    @Override
    public boolean isExperimental() {
        return true;
    }

    @Override
    public boolean suggestUnbindBeforePair() {
        return false;
    }

    @Override
    public boolean supportsDataFetching(@NonNull final GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsActivityTracking(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsContinuousTemperature(@NonNull final GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsHeartRateMeasurement(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsHrvMeasurement(@NonNull final GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsManualHeartRateMeasurement(@NonNull final GBDevice device) {
        return false;
    }

    @Override
    public boolean supportsRealtimeData(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsSleepMeasurement(@NonNull GBDevice device) {
        return false;
    }

    @Override
    public boolean supportsSpo2(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsStepCounter(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsTemperatureMeasurement(@NonNull final GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsSpeedzones(@NonNull GBDevice device) {
        return false;
    }

    @Override
    public boolean supportsStressMeasurement(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public DeviceKind getDeviceKind(@NonNull GBDevice device) {
        return DeviceKind.RING;
    }
}
