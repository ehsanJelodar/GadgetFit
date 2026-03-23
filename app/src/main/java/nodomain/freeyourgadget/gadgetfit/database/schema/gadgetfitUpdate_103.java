package nodomain.freeyourgadget.gadgetfit.database.schema;

import android.database.sqlite.SQLiteDatabase;

import nodomain.freeyourgadget.gadgetfit.database.DBHelper;
import nodomain.freeyourgadget.gadgetfit.database.DBUpdateScript;
import nodomain.freeyourgadget.gadgetfit.entities.HuaweiWorkoutSummarySampleDao;

public class gadgetfitUpdate_103 implements DBUpdateScript {
    @Override
    public void upgradeSchema(final SQLiteDatabase db) {
        if (!DBHelper.existsColumn(HuaweiWorkoutSummarySampleDao.TABLENAME, HuaweiWorkoutSummarySampleDao.Properties.LongestStreak.columnName, db)) {
            final String statement = "ALTER TABLE " + HuaweiWorkoutSummarySampleDao.TABLENAME + " ADD COLUMN \""
                    + HuaweiWorkoutSummarySampleDao.Properties.LongestStreak.columnName + "\" INTEGER NOT NULL DEFAULT -1;";
            db.execSQL(statement);
        }
        if (!DBHelper.existsColumn(HuaweiWorkoutSummarySampleDao.TABLENAME, HuaweiWorkoutSummarySampleDao.Properties.Tripped.columnName, db)) {
            final String statement = "ALTER TABLE " + HuaweiWorkoutSummarySampleDao.TABLENAME + " ADD COLUMN \""
                    + HuaweiWorkoutSummarySampleDao.Properties.Tripped.columnName + "\" INTEGER NOT NULL DEFAULT -1;";
            db.execSQL(statement);
        }
    }

    @Override
    public void downgradeSchema(SQLiteDatabase database) {

    }
}
