package nodomain.freeyourgadget.gadgetfit.devices.cycling_sensor.db;

import androidx.annotation.NonNull;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.CyclingSample;
import nodomain.freeyourgadget.gadgetfit.entities.CyclingSampleDao;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class CyclingSampleProvider extends AbstractTimeSampleProvider<CyclingSample> {
    public CyclingSampleProvider(GBDevice device, DaoSession session) {
        super(device, session);
    }

    @NonNull
    @Override
    public AbstractDao<CyclingSample, ?> getSampleDao() {
        return getSession().getCyclingSampleDao();
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return CyclingSampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return CyclingSampleDao.Properties.DeviceId;
    }

    @Override
    public CyclingSample createSample() {
        return new CyclingSample();
    }
}
