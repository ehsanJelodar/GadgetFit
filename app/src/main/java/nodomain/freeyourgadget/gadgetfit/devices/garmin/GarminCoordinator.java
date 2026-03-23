package nodomain.freeyourgadget.gadgetfit.devices.garmin;

import android.content.Context;
import android.content.SharedPreferences;
import android.net.Uri;
import android.app.Activity;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.GBApplication;
import nodomain.freeyourgadget.gadgetfit.R;
import nodomain.freeyourgadget.gadgetfit.activities.appmanager.AppManagerActivity;
import nodomain.freeyourgadget.gadgetfit.activities.appmanager.config.DynamicAppConfigActivity;
import nodomain.freeyourgadget.gadgetfit.activities.charts.DeviceChartsProvider;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettings;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettingsCustomizer;
import nodomain.freeyourgadget.gadgetfit.activities.devicesettings.DeviceSpecificSettingsScreen;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator;
import nodomain.freeyourgadget.gadgetfit.devices.GenericTrainingLoadAcuteSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.GenericTrainingLoadChronicSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.WorkoutLoadSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.WorkoutVo2MaxSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.InstallHandler;
import nodomain.freeyourgadget.gadgetfit.devices.SampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.devices.Vo2MaxSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.BaseActivitySummaryDao;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.GarminActivitySampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminBodyEnergySampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminEventSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminFitFileDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminHeartRateRestingSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminHrvSummarySampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminHrvValueSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminIntensityMinutesSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminNapSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminRespiratoryRateSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminRestingMetabolicRateSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminSleepStageSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminSleepStatsSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminSpo2SampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GarminStressSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GenericTrainingLoadAcuteSample;
import nodomain.freeyourgadget.gadgetfit.entities.GenericTrainingLoadAcuteSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.GenericTrainingLoadChronicSample;
import nodomain.freeyourgadget.gadgetfit.entities.GenericTrainingLoadChronicSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.PendingFileDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;
import nodomain.freeyourgadget.gadgetfit.impl.GBDeviceCandidate;
import nodomain.freeyourgadget.gadgetfit.model.ActivitySample;
import nodomain.freeyourgadget.gadgetfit.model.ActivitySummaryParser;
import nodomain.freeyourgadget.gadgetfit.model.BodyEnergySample;
import nodomain.freeyourgadget.gadgetfit.model.DeviceType;
import nodomain.freeyourgadget.gadgetfit.model.HrvSummarySample;
import nodomain.freeyourgadget.gadgetfit.model.HrvValueSample;
import nodomain.freeyourgadget.gadgetfit.model.PaiSample;
import nodomain.freeyourgadget.gadgetfit.model.RespiratoryRateSample;
import nodomain.freeyourgadget.gadgetfit.model.RestingMetabolicRateSample;
import nodomain.freeyourgadget.gadgetfit.model.SleepScoreSample;
import nodomain.freeyourgadget.gadgetfit.model.Spo2Sample;
import nodomain.freeyourgadget.gadgetfit.model.StressSample;
import nodomain.freeyourgadget.gadgetfit.model.Vo2MaxSample;
import nodomain.freeyourgadget.gadgetfit.model.WorkoutLoadSample;
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport;
import nodomain.freeyourgadget.gadgetfit.service.devices.garmin.GarminSupport;
import nodomain.freeyourgadget.gadgetfit.util.Prefs;
import nodomain.freeyourgadget.gadgetfit.util.preferences.DevicePrefs;

public abstract class GarminCoordinator extends AbstractBLEDeviceCoordinator {
    @Override
    public boolean suggestUnbindBeforePair() {
        return false;
    }

    @Override
    public GBDevice createDevice(final GBDeviceCandidate candidate, final DeviceType deviceType) {
        final GBDevice gbDevice = super.createDevice(candidate, deviceType);

        if (defaultNewSyncProtocol()) {
            final DevicePrefs devicePreferences = GBApplication.getDevicePrefs(gbDevice);
            final SharedPreferences.Editor editor = devicePreferences.getPreferences().edit();

            // #5021 - Some new devices like Venu X1 misses a lot of files without the new sync protocol
            editor.putBoolean("new_sync_protocol", true);

            editor.apply();
        }

        return gbDevice;
    }

    public boolean defaultNewSyncProtocol() {
        return false;
    }

    @Override
    public Map<AbstractDao<?, ?>, Property> getAllDeviceDao( @NonNull final DaoSession session) {
        return new HashMap<>() {{
            put(session.getGarminActivitySampleDao(), GarminActivitySampleDao.Properties.DeviceId);
            put(session.getGarminStressSampleDao(), GarminStressSampleDao.Properties.DeviceId);
            put(session.getGarminBodyEnergySampleDao(), GarminBodyEnergySampleDao.Properties.DeviceId);
            put(session.getGarminSpo2SampleDao(), GarminSpo2SampleDao.Properties.DeviceId);
            put(session.getGarminSleepStageSampleDao(), GarminSleepStageSampleDao.Properties.DeviceId);
            put(session.getGarminEventSampleDao(), GarminEventSampleDao.Properties.DeviceId);
            put(session.getGarminFitFileDao(), GarminFitFileDao.Properties.DeviceId);
            put(session.getGarminHeartRateRestingSampleDao(), GarminHeartRateRestingSampleDao.Properties.DeviceId);
            put(session.getGarminHrvSummarySampleDao(), GarminHrvSummarySampleDao.Properties.DeviceId);
            put(session.getGarminHrvValueSampleDao(), GarminHrvValueSampleDao.Properties.DeviceId);
            put(session.getGarminIntensityMinutesSampleDao(), GarminIntensityMinutesSampleDao.Properties.DeviceId);
            put(session.getGarminNapSampleDao(), GarminNapSampleDao.Properties.DeviceId);
            put(session.getGarminRespiratoryRateSampleDao(), GarminRespiratoryRateSampleDao.Properties.DeviceId);
            put(session.getGarminRestingMetabolicRateSampleDao(), GarminRestingMetabolicRateSampleDao.Properties.DeviceId);
            put(session.getGarminSleepStatsSampleDao(), GarminSleepStatsSampleDao.Properties.DeviceId);
            put(session.getBaseActivitySummaryDao(), BaseActivitySummaryDao.Properties.DeviceId);
            put(session.getPendingFileDao(), PendingFileDao.Properties.DeviceId);
            put(session.getGenericTrainingLoadAcuteSampleDao(), GenericTrainingLoadAcuteSampleDao.Properties.DeviceId);
            put(session.getGenericTrainingLoadChronicSampleDao(), GenericTrainingLoadChronicSampleDao.Properties.DeviceId);
        }};
    }

    @Override
    public String getManufacturer() {
        return "Garmin";
    }

    @NonNull
    @Override
    public Class<? extends DeviceSupport> getDeviceSupportClass(final GBDevice device) {
        return GarminSupport.class;
    }

    @Override
    public DeviceChartsProvider getChartsProvider() {
        return new GarminChartsProvider();
    }

    @Nullable
    @Override
    public ActivitySummaryParser getActivitySummaryParser(final GBDevice device, final Context context) {
        return new GarminWorkoutParser(context);
    }

    @Override
    public SampleProvider<? extends ActivitySample> getSampleProvider(final GBDevice device, DaoSession session) {
        return new GarminActivitySampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends StressSample> getStressSampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminStressSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends BodyEnergySample> getBodyEnergySampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminBodyEnergySampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends HrvSummarySample> getHrvSummarySampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminHrvSummarySampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends HrvValueSample> getHrvValueSampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminHrvValueSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends WorkoutLoadSample> getWorkoutLoadSampleProvider(final GBDevice device, final DaoSession session) {
        return new WorkoutLoadSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends GenericTrainingLoadAcuteSample> getTrainingAcuteLoadSampleProvider(final GBDevice device, final DaoSession session) {
        return new GenericTrainingLoadAcuteSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends GenericTrainingLoadChronicSample> getTrainingChronicLoadSampleProvider(final GBDevice device, final DaoSession session) {
        return new GenericTrainingLoadChronicSampleProvider(device, session);
    }

    @Override
    public Vo2MaxSampleProvider<? extends Vo2MaxSample> getVo2MaxSampleProvider(final GBDevice device, final DaoSession session) {
        return new WorkoutVo2MaxSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends Spo2Sample> getSpo2SampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminSpo2SampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends PaiSample> getPaiSampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminPaiSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends RespiratoryRateSample> getRespiratoryRateSampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminRespiratoryRateSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends RestingMetabolicRateSample> getRestingMetabolicRateProvider(final GBDevice device, final DaoSession session) {
        return new GarminRestingMetabolicRateSampleProvider(device, session);
    }

    @Override
    public TimeSampleProvider<? extends SleepScoreSample> getSleepScoreProvider(final GBDevice device, final DaoSession session) {
        return new GarminSleepStatsSampleProvider(device, session);
    }

    @Override
    public GarminHeartRateRestingSampleProvider getHeartRateRestingSampleProvider(final GBDevice device, final DaoSession session) {
        return new GarminHeartRateRestingSampleProvider(device, session);
    }

    @Override
    public int[] getSupportedDeviceSpecificAuthenticationSettings() {
        return new int[]{R.xml.devicesettings_garmin_fake_oauth};
    }

    @Override
    public int[] getSupportedDeviceSpecificExperimentalSettings(final GBDevice device) {
        return new int[]{R.xml.devicesettings_garmin_experimental};
    }

    @Override
    public int[] getSupportedDebugSettings(final GBDevice device) {
        return ArrayUtils.add(
                super.getSupportedDebugSettings(device),
                R.xml.devicesettings_debug_drop_packets
        );
    }

    @Override
    public DeviceSpecificSettings getDeviceSpecificSettings(final GBDevice device) {
        final DeviceSpecificSettings deviceSpecificSettings = new DeviceSpecificSettings();

        if (supports(device, GarminCapability.REALTIME_SETTINGS)) {
            deviceSpecificSettings.addRootScreen(R.xml.devicesettings_garmin_realtime_settings);
        }

        if (supportsCalendarEvents(device)){
            deviceSpecificSettings.addRootScreen(
                    DeviceSpecificSettingsScreen.CALENDAR,
                    R.xml.devicesettings_header_calendar,
                    R.xml.devicesettings_sync_calendar
            );
        }

        final List<Integer> notifications = deviceSpecificSettings.addRootScreen(DeviceSpecificSettingsScreen.CALLS_AND_NOTIFICATIONS);

        notifications.add(R.xml.devicesettings_send_app_notifications);

        notifications.add(R.xml.devicesettings_transliteration);

        if (getCannedRepliesSlotCount(device) > 0) {
            notifications.add(R.xml.devicesettings_canned_reply_16);
            notifications.add(R.xml.devicesettings_canned_dismisscall_16);
        }
        if (getContactsSlotCount(device) > 0) {
            notifications.add(R.xml.devicesettings_contacts);
        }

        final List<Integer> location = deviceSpecificSettings.addRootScreen(DeviceSpecificSettingsScreen.LOCATION);
        location.add(R.xml.devicesettings_workout_send_gps_to_band);
        if (supportsAgpsUpdates(device)) {
            location.add(R.xml.devicesettings_garmin_agps);
        }

        final List<Integer> connection = deviceSpecificSettings.addRootScreen(DeviceSpecificSettingsScreen.CONNECTION);
        connection.add(R.xml.devicesettings_high_mtu);

        if (GBApplication.hasInternetAccess()) {
            final List<Integer> internet = deviceSpecificSettings.addRootScreen(DeviceSpecificSettingsScreen.INTERNET);
            internet.add(R.xml.devicesettings_device_internet_access);
            internet.add(R.xml.devicesettings_device_internet_firewall);
        }

        final List<Integer> developer = deviceSpecificSettings.addRootScreen(DeviceSpecificSettingsScreen.DEVELOPER);
        developer.add(R.xml.devicesettings_import_activity_files);
        developer.add(R.xml.devicesettings_reprocess_activity_files);
        developer.add(R.xml.devicesettings_keep_activity_data_on_device);
        developer.add(R.xml.devicesettings_fetch_unknown_files);
        developer.add(R.xml.devicesettings_install_unsupported_files);
        developer.add(R.xml.devicesettings_new_sync_protocol);
        developer.add(R.xml.devicesettings_garmin_mlr);

        return deviceSpecificSettings;
    }

    @Override
    public DeviceSpecificSettingsCustomizer getDeviceSpecificSettingsCustomizer(GBDevice device) {
        return new GarminSettingsCustomizer();
    }

    @Override
    public int getCannedRepliesSlotCount(final GBDevice device) {
        if (getPrefs(device).getBoolean(GarminPreferences.PREF_FEAT_CANNED_MESSAGES, false)) {
            return 16;
        }

        return 0;
    }

    @Override
    public int getContactsSlotCount(final GBDevice device) {
        if (getPrefs(device).getBoolean(GarminPreferences.PREF_FEAT_CONTACTS, false)) {
            return 50;
        }

        return 0;
    }

    protected static Prefs getPrefs(final GBDevice device) {
        return new Prefs(GBApplication.getDeviceSpecificSharedPrefs(device.getAddress()));
    }

    @Override
    public boolean supportsUnicodeEmojis(@NonNull GBDevice device) {
        return true;
    }

    @Override
    public boolean supportsAppsManagement(@NonNull final GBDevice device) {
        // FIXME: experimental until better polished
        return experimentalSettingEnabled(device, "garmin_experimental_app_management");
    }

    @Override
    public boolean supportsCachedAppManagement(@NonNull final GBDevice device) {
        return false;
    }

    @Override
    public Class<? extends Activity> getAppsManagementActivity(final GBDevice device) {
        return AppManagerActivity.class;
    }

    @Nullable
    @Override
    public Class<? extends Activity> getAppConfigurationActivity(final GBDevice device) {
        return DynamicAppConfigActivity.class;
    }

    @Override
    public boolean supportsAppListFetching(@NonNull final GBDevice device) {
        return true;
    }

    public boolean supportsAgpsUpdates(final GBDevice device) {
        return !getPrefs(device).getString(GarminPreferences.PREF_AGPS_KNOWN_URLS, "").isEmpty();
    }

    public boolean supports(final GBDevice device, final GarminCapability capability) {
        return getPrefs(device).getStringSet(GarminPreferences.PREF_GARMIN_CAPABILITIES, Collections.emptySet())
                .contains(capability.name());
    }

    @Override
    public boolean supportsFlashing(@NonNull GBDevice device) {
        return true;
    }

    @Nullable
    @Override
    public InstallHandler findInstallHandler(Uri uri, Context context) {
        final GarminFitFileInstallHandler fitFileInstallHandler = new GarminFitFileInstallHandler(uri, context);
        if (fitFileInstallHandler.isValid())
            return fitFileInstallHandler;

        final GarminGpxRouteInstallHandler garminGpxRouteInstallHandler = new GarminGpxRouteInstallHandler(uri, context);
        if (garminGpxRouteInstallHandler.isValid())
            return garminGpxRouteInstallHandler;

        final GarminPrgFileInstallHandler prgFileInstallHandler = new GarminPrgFileInstallHandler(uri, context);
        if (prgFileInstallHandler.isValid())
            return prgFileInstallHandler;

        return null;
    }

    @Override
    public boolean supportsTrainingLoad(@NonNull final GBDevice device) {
        // Not all devices support it
        return true;
    }
}
