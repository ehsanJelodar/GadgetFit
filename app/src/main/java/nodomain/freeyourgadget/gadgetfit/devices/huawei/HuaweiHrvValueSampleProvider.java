package nodomain.freeyourgadget.gadgetfit.devices.huawei;

import androidx.annotation.NonNull;

import java.util.List;

import de.greenrobot.dao.AbstractDao;
import de.greenrobot.dao.Property;
import de.greenrobot.dao.query.QueryBuilder;
import nodomain.freeyourgadget.gadgetfit.database.DBHelper;
import nodomain.freeyourgadget.gadgetfit.devices.AbstractTimeSampleProvider;
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession;
import nodomain.freeyourgadget.gadgetfit.entities.Device;
import nodomain.freeyourgadget.gadgetfit.entities.HuaweiHrvValueSample;
import nodomain.freeyourgadget.gadgetfit.entities.HuaweiHrvValueSampleDao;
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice;

public class HuaweiHrvValueSampleProvider extends AbstractTimeSampleProvider<HuaweiHrvValueSample> {
    public HuaweiHrvValueSampleProvider(final GBDevice device, final DaoSession session) {
        super(device, session);
    }

    @NonNull
    @Override
    public AbstractDao<HuaweiHrvValueSample, ?> getSampleDao() {
        return getSession().getHuaweiHrvValueSampleDao();
    }

    @NonNull
    @Override
    protected Property getTimestampSampleProperty() {
        return HuaweiHrvValueSampleDao.Properties.Timestamp;
    }

    @NonNull
    @Override
    protected Property getDeviceIdentifierSampleProperty() {
        return HuaweiHrvValueSampleDao.Properties.DeviceId;
    }

    @Override
    public HuaweiHrvValueSample createSample() {
        return new HuaweiHrvValueSample();
    }


    public long getLastFetchTimestamp() {
        QueryBuilder<HuaweiHrvValueSample> qb = getSampleDao().queryBuilder();
        Device dbDevice = DBHelper.findDevice(getDevice(), getSession());
        if (dbDevice == null)
            return 0;
        final Property deviceProperty = HuaweiHrvValueSampleDao.Properties.DeviceId;
        final Property timestampProperty = HuaweiHrvValueSampleDao.Properties.LastTimestamp;

        qb.where(deviceProperty.eq(dbDevice.getId()))
                .orderDesc(timestampProperty)
                .limit(1);

        List<HuaweiHrvValueSample> samples = qb.build().list();
        if (samples.isEmpty())
            return 0;

        HuaweiHrvValueSample sample = samples.get(0);
        return sample.getLastTimestamp();
    }

}
