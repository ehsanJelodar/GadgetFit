package nodomain.freeyourgadget.gadgetfit.database.schema;

import android.database.sqlite.SQLiteDatabase;

import nodomain.freeyourgadget.gadgetfit.database.DBUpdateScript;
import nodomain.freeyourgadget.gadgetfit.entities.HuaweiSleepStatsSampleDao;

public class gadgetfitUpdate_109 implements DBUpdateScript {
    @Override
    public void upgradeSchema(SQLiteDatabase db) {
        String IDX_NAME = "IDX_" + HuaweiSleepStatsSampleDao.TABLENAME + "_" + HuaweiSleepStatsSampleDao.Properties.WakeupTime.columnName;
        String CREATE_WAKEUP_TIME_INDEX = "CREATE INDEX IF NOT EXISTS " + IDX_NAME + " ON " + HuaweiSleepStatsSampleDao.TABLENAME + "("
                + HuaweiSleepStatsSampleDao.Properties.WakeupTime.columnName + ");";

        db.execSQL(CREATE_WAKEUP_TIME_INDEX);
    }

    @Override
    public void downgradeSchema(SQLiteDatabase db) {
    }
}
