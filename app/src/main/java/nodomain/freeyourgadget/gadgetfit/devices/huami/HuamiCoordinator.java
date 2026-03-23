/*  Copyright (C) 2017-2024 Andreas Shimokawa, Damien Gaignon, Daniel Dakhno,
    Daniele Gobbetti, José Rebelo, NekoBox, Nephiel, Petr Vaněk

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
package nodomain.freeyourgadget.gadgetfit.devices.huami;

import android.app.Activity;
import android.bluetooth.le.ScanFilter;
import android.content.Context;
import android.content.SharedPreferences;
import android.os.ParcelUuid;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.SettingsActivity;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSettingsPreferenceConst;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettingsCustomizer;
import nodomain.freeyourgadget.gadgetfit.capabilities.password.PasswordCapabilityImpl;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.ComputedHrvSummarySampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.GenericHrvValueSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.GenericTemperatureSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.SampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.miband.DateTimeDisplay;
import nodomain.freeyourgadget.gadgetfit.devices.miband.DoNotDisturb;
import nodomain.freeyourgadget.gadgetfit.devices.miband.MiBand2SampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.miband.MiBandConst;
import nodomain.freeyourgadget.gadgetfit.devices.miband.MiBandPairingActivity;
import nodomain.freeyourgadget.gadgetfit.devices.miband.MiBandService;
import nodomain.freeyourgadget.gadgetfit.devices.miband.VibrationProfile;
import nodomain.freeyourgadget.gadgetfit.entities.AbstractActivitySample;
import nodomain.freeyourgadget.gadgetfit.entities.AudioRecordingDao;
import nodomain.freeyourgadget.gadgetfit.entities.BaseActivitySummaryDao;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.GenericHrvValueSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GenericTemperatureSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiExtendedActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiHeartRateManualSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiHeartRateMaxSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiHeartRateRestingSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiPaiSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiSleepRespiratoryRateSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiSleepSessionSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiSpo2SampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.HuamiStressSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.MiBandActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.model.ActivitySummaryParser;
import nodomain.freeyourgadget.gadgetfit.model.HrvSummarySample;
import nodomain.freeyourgadget.gadgetfit.model.SleepScoreSample;
import nodomain.freeyourgadget.gadgetfit.model.TemperatureSample;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.HuamiVibrationPatternNotificationType;
import nodomain.freeyourgadget.gadgetfit.util.Prefs;

public abstract class HuamiCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    public Class<? extends Activity> getPairingActivity() {
        return MiBandPairingActivity.class;
    }

    @NonNull
    @Override
    public Collection<? extends ScanFilter> createBLEScanFilters() {
        ParcelUuid mi2Service = new ParcelUuid(MiBandService.UUID_SERVICE_MIBAND2_SERVICE);
        ScanFilter filter = new ScanFilter.Builder().setServiceUuid(mi2Service).build();
        return Collections.singletonList(filter);
    }

    @Override
    public boolean suggestUnbindBeforePair() {
        return false;
    }

    @Nullable
    @Override
    public String getAuthHelp() {
        return "https://gadgetfit.org/basics/pairing/huami-xiaomi-server/";
    }

    @Override
    public Map<AbstractDao<?, ?>, Property> getAllDeviceDao(@NonNull final DaoSession session) {
        return new HashMap<>() {{
            put(session.getMiBandActivitySampleDao(), MiBandActivitySampleDao.Properties.DeviceId);
            put(session.getHuamiExtendedActivitySampleDao(), HuamiExtendedActivitySampleDao.Properties.DeviceId);
            put(session.getHuamiStressSampleDao(), HuamiStressSampleDao.Properties.DeviceId);
            put(session.getHuamiSpo2SampleDao(), HuamiSpo2SampleDao.Properties.DeviceId);
            put(session.getHuamiHeartRateManualSampleDao(), HuamiHeartRateManualSampleDao.Properties.DeviceId);
            put(session.getHuamiHeartRateMaxSampleDao(), HuamiHeartRateMaxSampleDao.Properties.DeviceId);
            put(session.getHuamiHeartRateRestingSampleDao(), HuamiHeartRateRestingSampleDao.Properties.DeviceId);
            put(session.getHuamiPaiSampleDao(), HuamiPaiSampleDao.Properties.DeviceId);
            put(session.getHuamiSleepRespiratoryRateSampleDao(), HuamiSleepRespiratoryRateSampleDao.Properties.DeviceId);
            put(session.getGenericHrvValueSampleDao(), GenericHrvValueSampleDao.Properties.DeviceId);
            put(session.getGenericTemperatureSampleDao(), GenericTemperatureSampleDao.Properties.DeviceId);
            put(session.getHuamiSleepSessionSampleDao(), HuamiSleepSessionSampleDao.Properties.DeviceId);
            put(session.getBaseActivitySummaryDao(), BaseActivitySummaryDao.Properties.DeviceId);
            put(session.getAudioRecordingDao(), AudioRecordingDao.Properties.DeviceId);
        }};
    }

    @Override
    public String getManufacturer() {
        return "Huami";
    }

    @Override
    public boolean supportsFlashing(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsRealtimeData(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public int getAlarmSlotCount(GBDevice device) {
        return 10;
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
    public boolean supportsHeartRateRestingMeasurement(@NonNull final GBDevice device) {
        return true;
    }

    @Override
    public int[] getSupportedDeviceSpecificAuthenticationSettings() {
        return new int[]{R.xml.devicesettings_pairingkey};
    }

    @Override
    public SampleProvider<? extends AbstractActivitySample> getSampleProvider(GBDevice device, DaoSession session) {
        return new MiBand2SampleProvider(device, session);
    }

    @Override
    public HuamiStressSampleProvider getStressSampleProvider(final GBDevice device, final  DaoSession session) {
        return new HuamiStressSampleProvider(device, session);
    }

    @Override
    public HuamiSpo2SampleProvider getSpo2SampleProvider(final GBDevice device, final  DaoSession session) {
        return new HuamiSpo2SampleProvider(device, session);
    }

    @Override
    public HuamiHeartRateMaxSampleProvider getHeartRateMaxSampleProvider(final GBDevice device, final DaoSession session) {
        return new HuamiHeartRateMaxSampleProvider(device, session);
    }

    @Override
    public HuamiHeartRateRestingSampleProvider getHeartRateRestingSampleProvider(final GBDevice device, final DaoSession session) {
        return new HuamiHeartRateRestingSampleProvider(device, session);
    }

    @Override
    public HuamiHeartRateManualSampleProvider getHeartRateManualSampleProvider(final GBDevice device, final DaoSession session) {
        return new HuamiHeartRateManualSampleProvider(device, session);
    }

    @Override
    public HuamiPaiSampleProvider getPaiSampleProvider(GBDevice device, DaoSession session) {
        return new HuamiPaiSampleProvider(device, session);
    }

    @Override
    public HuamiSleepRespiratoryRateSampleProvider getRespiratoryRateSampleProvider(GBDevice device, DaoSession session) {
        return new HuamiSleepRespiratoryRateSampleProvider(device, session);
    }

    @Override
    public GenericHrvValueSampleProvider getHrvValueSampleProvider(GBDevice device, DaoSession session) {
        return new GenericHrvValueSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends HrvSummarySample> getHrvSummarySampleProvider(GBDevice device, DaoSession session) {
        return new ComputedHrvSummarySampleProvider(getHrvValueSampleProvider(device, session), device, session);
    }

    @Override
    public GenericTemperatureSampleProvider getTemperatureSampleProvider(GBDevice device, DaoSession session) {
        return new GenericTemperatureSampleProvider(device, session, TemperatureSample.TYPE_SKIN, TemperatureSample.LOCATION_WRIST);
    }

    @Override
    public TimeSampleProvider<? extends SleepScoreSample> getSleepScoreProvider(final GBDevice device, final DaoSession session) {
        return new HuamiSleepSessionSampleProvider(device, session);
    }

    @Override
    public ActivitySummaryParser getActivitySummaryParser(final GBDevice device, final Context context) {
        return new HuamiActivitySummaryParser();
    }

    protected static Prefs getPrefs(final GBDevice device) {
        return new Prefs(GBApplication.getDeviceSpecificSharedPrefs(device.getAddress()));
    }

    public static DateTimeDisplay getDateDisplay(Context context, String deviceAddress) throws IllegalArgumentException {
        SharedPreferences sharedPrefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        String dateFormatTime = context.getString(R.string.p_dateformat_time);
        if (dateFormatTime.equals(sharedPrefs.getString(MiBandConst.PREF_MI2_DATEFORMAT, dateFormatTime))) {
            return DateTimeDisplay.TIME;
        }
        return DateTimeDisplay.DATE_TIME;
    }

    public static ActivateDisplayOnLift getActivateDisplayOnLiftWrist(Context context, String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        String liftOff = context.getString(R.string.p_off);
        String pref = prefs.getString(DeviceSettingsPreferenceConst.PREF_ACTIVATE_DISPLAY_ON_LIFT, liftOff);

        return ActivateDisplayOnLift.valueOf(pref.toUpperCase(Locale.ROOT));
    }

    public static Date getDisplayOnLiftStart(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_DISPLAY_ON_LIFT_START, "00:00", deviceAddress);
    }

    public static Date getDisplayOnLiftEnd(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_DISPLAY_ON_LIFT_END, "00:00", deviceAddress);
    }

    public static ActivateDisplayOnLiftSensitivity getDisplayOnLiftSensitivity(String deviceAddress) {
        final SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);

        final String pref = prefs.getString(DeviceSettingsPreferenceConst.PREF_DISPLAY_ON_LIFT_SENSITIVITY, "sensitive");

        return ActivateDisplayOnLiftSensitivity.valueOf(pref.toUpperCase(Locale.ROOT));
    }

    public static DisconnectNotificationSetting getDisconnectNotificationSetting(Context context, String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        String liftOff = context.getString(R.string.p_off);
        String pref = prefs.getString(DeviceSettingsPreferenceConst.PREF_DISCONNECT_NOTIFICATION, liftOff);

        return DisconnectNotificationSetting.valueOf(pref.toUpperCase(Locale.ROOT));
    }

    public static Date getDisconnectNotificationStart(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_DISCONNECT_NOTIFICATION_START, "00:00", deviceAddress);
    }

    public static Date getDisconnectNotificationEnd(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_DISCONNECT_NOTIFICATION_END, "00:00", deviceAddress);
    }

    public static boolean getUseCustomFont(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        return prefs.getBoolean(HuamiConst.PREF_USE_CUSTOM_FONT, false);
    }

    public static boolean getGoalNotification(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_USER_FITNESS_GOAL_NOTIFICATION, false);
    }

    public static boolean getRotateWristToSwitchInfo(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        return prefs.getBoolean(MiBandConst.PREF_MI2_ROTATE_WRIST_TO_SWITCH_INFO, false);
    }

    public static boolean getInactivityWarnings(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_INACTIVITY_ENABLE, false);
    }

    public static int getInactivityWarningsThreshold(String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getInt(DeviceSettingsPreferenceConst.PREF_INACTIVITY_THRESHOLD, 60);
    }

    public static boolean getInactivityWarningsDnd(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_INACTIVITY_DND, false);
    }

    public static Date getInactivityWarningsStart(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_INACTIVITY_START, "06:00", deviceAddress);
    }

    public static Date getInactivityWarningsEnd(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_INACTIVITY_END, "22:00", deviceAddress);
    }

    public static Date getInactivityWarningsDndStart(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_INACTIVITY_DND_START, "12:00", deviceAddress);
    }

    public static Date getInactivityWarningsDndEnd(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_INACTIVITY_DND_END, "14:00", deviceAddress);
    }

    public static Date getDoNotDisturbStart(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_DO_NOT_DISTURB_START, "01:00", deviceAddress);
    }

    public static Date getDoNotDisturbEnd(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_DO_NOT_DISTURB_END, "06:00", deviceAddress);
    }

    public static boolean getBandScreenUnlock(String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean(MiBandConst.PREF_SWIPE_UNLOCK, false);
    }

    public static boolean getExposeHRThirdParty(String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean(HuamiConst.PREF_EXPOSE_HR_THIRDPARTY, false);
    }

    public static int getHeartRateMeasurementInterval(String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getInt(DeviceSettingsPreferenceConst.PREF_HEARTRATE_MEASUREMENT_INTERVAL, 0) / 60;
    }

    public static boolean getHeartrateActivityMonitoring(String deviceAddress) throws IllegalArgumentException {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_HEARTRATE_ACTIVITY_MONITORING, false);
    }

    public static boolean getPasswordEnabled(String deviceAddress) throws IllegalArgumentException {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean(PasswordCapabilityImpl.PREF_PASSWORD_ENABLED, false);
    }

    public static String getPassword(String deviceAddress) throws IllegalArgumentException {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getString(PasswordCapabilityImpl.PREF_PASSWORD, null);
    }

    public static boolean getHeartrateAlert(String deviceAddress) throws IllegalArgumentException {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_HEARTRATE_ALERT_ENABLED, false);
    }

    public static int getHeartrateAlertHighThreshold(String deviceAddress) throws IllegalArgumentException {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getInt(DeviceSettingsPreferenceConst.PREF_HEARTRATE_ALERT_HIGH_THRESHOLD, 150);
    }

    public static boolean getHeartrateStressMonitoring(String deviceAddress) throws IllegalArgumentException {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_HEARTRATE_STRESS_MONITORING, false);
    }

    public static boolean getBtConnectedAdvertising(String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_BT_CONNECTED_ADVERTISEMENT, false);
    }

    public static boolean getOverwriteSettingsOnConnection(String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean("overwrite_settings_on_connection", true);
    }

    public static boolean getKeepActivityDataOnDevice(String deviceAddress) {
        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        return prefs.getBoolean("keep_activity_data_on_device", false);
    }

    @Nullable
    public static VibrationProfile getVibrationProfile(String deviceAddress, HuamiVibrationPatternNotificationType notificationType, boolean nullOnDeviceDefault) {
        final String defaultVibrationProfileId;
        final int defaultVibrationCount = switch (notificationType) {
            case APP_ALERTS, TODO_LIST, SCHEDULE -> {
                defaultVibrationProfileId = VibrationProfile.ID_SHORT;
                yield 2;
            }
            case INCOMING_CALL -> {
                defaultVibrationProfileId = VibrationProfile.ID_RING;
                yield 1;
            }
            case INCOMING_SMS -> {
                defaultVibrationProfileId = VibrationProfile.ID_STACCATO;
                yield 2;
            }
            case GOAL_NOTIFICATION, EVENT_REMINDER -> {
                defaultVibrationProfileId = VibrationProfile.ID_LONG;
                yield 1;
            }
            case ALARM -> {
                defaultVibrationProfileId = VibrationProfile.ID_LONG;
                yield 7;
            }
            case FIND_BAND -> {
                defaultVibrationProfileId = VibrationProfile.ID_RING;
                yield 3;
            }
            default -> {
                defaultVibrationProfileId = VibrationProfile.ID_MEDIUM;
                yield 2;
            }
        };

        Prefs prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        final String vibrationProfileId = prefs.getString(
                HuamiConst.PREF_HUAMI_VIBRATION_PROFILE_PREFIX + notificationType.name().toLowerCase(Locale.ROOT),
                HuamiConst.PREF_HUAMI_DEFAULT_VIBRATION_PROFILE
        );

        if (HuamiConst.PREF_HUAMI_DEFAULT_VIBRATION_PROFILE.equals(vibrationProfileId)) {
            if (nullOnDeviceDefault) {
                // Return null, so the device default is used
                return null;
            }

            return VibrationProfile.getProfile(defaultVibrationProfileId, (short) defaultVibrationCount);
        }

        final int vibrationProfileCount = prefs.getInt(HuamiConst.PREF_HUAMI_VIBRATION_COUNT_PREFIX + notificationType.name().toLowerCase(Locale.ROOT), defaultVibrationCount);

        return VibrationProfile.getProfile(vibrationProfileId, (short) vibrationProfileCount);
    }

    protected static Date getTimePreference(String key, String defaultValue, String deviceAddress) {
        Prefs prefs;

        if (deviceAddress == null) {
            prefs = GBApplication.getPrefs();
        } else {
            prefs = new Prefs(GBApplication.getDeviceSpecificSharedPrefs(deviceAddress));
        }

        return prefs.getTimePreference(key, defaultValue);
    }

    public static MiBandConst.DistanceUnit getDistanceUnit() {
        Prefs prefs = GBApplication.getPrefs();
        String unit = prefs.getString(SettingsActivity.PREF_MEASUREMENT_SYSTEM, GBApplication.getContext().getString(R.string.p_unit_metric));
        if (unit.equals(GBApplication.getContext().getString(R.string.p_unit_metric))) {
            return MiBandConst.DistanceUnit.METRIC;
        } else {
            return MiBandConst.DistanceUnit.IMPERIAL;
        }
    }

    public static DoNotDisturb getDoNotDisturb(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);

        String pref = prefs.getString(DeviceSettingsPreferenceConst.PREF_DO_NOT_DISTURB, DeviceSettingsPreferenceConst.PREF_DO_NOT_DISTURB_OFF);

        return DoNotDisturb.valueOf(pref.toUpperCase(Locale.ROOT));
    }

    public static boolean getDoNotDisturbLiftWrist(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);

        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_DO_NOT_DISTURB_LIFT_WRIST, false);
    }

    public static boolean getWorkoutStartOnPhone(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);

        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_WORKOUT_START_ON_PHONE, false);
    }

    public static boolean getWorkoutSendGpsToBand(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);

        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_WORKOUT_SEND_GPS_TO_BAND, false);
    }

    @Override
    public boolean supportsFindDevice(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsAlarmSnoozing(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public int getMaximumReminderMessageLength() {
        return 16;
    }

    @Override
    public int getReminderSlotCount(final GBDevice device) {
        return 22; // At least, Mi Fit still allows more
    }

    @Override
    public boolean supportsCalendarEvents(@NonNull final GBDevice device) {
        return true;
    }

    @Override
    public boolean getReserveReminderSlotsForCalendar() {
        return true;
    }

    @Override
    public boolean supportsDebugLogs(@NonNull GBDevice device) {
        return true;
    }

    public List<HuamiVibrationPatternNotificationType> getVibrationPatternNotificationTypes(final GBDevice device) {
        return Arrays.asList(
                HuamiVibrationPatternNotificationType.APP_ALERTS,
                HuamiVibrationPatternNotificationType.INCOMING_CALL,
                HuamiVibrationPatternNotificationType.INCOMING_SMS,
                HuamiVibrationPatternNotificationType.GOAL_NOTIFICATION,
                HuamiVibrationPatternNotificationType.ALARM,
                HuamiVibrationPatternNotificationType.IDLE_ALERTS,
                HuamiVibrationPatternNotificationType.EVENT_REMINDER,
                HuamiVibrationPatternNotificationType.FIND_BAND
        );
    }

    @Override
    public DeviceSpecificSettingsCustomizer getDeviceSpecificSettingsCustomizer(final GBDevice device) {
        return new HuamiSettingsCustomizer(device, getVibrationPatternNotificationTypes(device));
    }

    public static boolean getHourlyChime(String deviceAddress) {
        SharedPreferences prefs = GBApplication.getDeviceSpecificSharedPrefs(deviceAddress);
        return prefs.getBoolean(DeviceSettingsPreferenceConst.PREF_HOURLY_CHIME_ENABLE, false);
    }

    public static Date getHourlyChimeStart(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_HOURLY_CHIME_START, "09:00", deviceAddress);
    }

    public static Date getHourlyChimeEnd(String deviceAddress) {
        return getTimePreference(DeviceSettingsPreferenceConst.PREF_HOURLY_CHIME_END, "22:00", deviceAddress);
    }

    @Override
    public int getDefaultIconResource() {
        return R.drawable.ic_device_zetime;
    }
}
