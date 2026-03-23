package nodomain.freeyourgadget.gadgetfit.database.schema;

import android.database.sqlite.SQLiteDatabase;

import nodomain.freeyourgadget.gadgetfit.database.DBHelper;
import nodomain.freeyourgadget.gadgetfit.database.DBUpdateScript;
import nodomain.freeyourgadget.gadgetfit.entities.XiaomiActivitySampleDao;

public class gadgetfitUpdate_117 implements DBUpdateScript {
    @Override
    public void upgradeSchema(final SQLiteDatabase db) {
        if (!DBHelper.existsColumn(XiaomiActivitySampleDao.TABLENAME, XiaomiActivitySampleDao.Properties.ActiveCalories.columnName, db)) {
            final String statement = "ALTER TABLE " + XiaomiActivitySampleDao.TABLENAME + " ADD COLUMN \""
                    + XiaomiActivitySampleDao.Properties.ActiveCalories.columnName + "\" INTEGER NOT NULL DEFAULT -1;";
            db.execSQL(statement);
        }

        if (!DBHelper.existsColumn(XiaomiActivitySampleDao.TABLENAME, XiaomiActivitySampleDao.Properties.DistanceCm.columnName, db)) {
            final String statement = "ALTER TABLE " + XiaomiActivitySampleDao.TABLENAME + " ADD COLUMN \""
                    + XiaomiActivitySampleDao.Properties.DistanceCm.columnName + "\" INTEGER NOT NULL DEFAULT -1;";
            db.execSQL(statement);
        }

        if (!DBHelper.existsColumn(XiaomiActivitySampleDao.TABLENAME, XiaomiActivitySampleDao.Properties.Energy.columnName, db)) {
            final String statement = "ALTER TABLE " + XiaomiActivitySampleDao.TABLENAME + " ADD COLUMN \""
                    + XiaomiActivitySampleDao.Properties.Energy.columnName + "\" INTEGER NOT NULL DEFAULT -1;";
            db.execSQL(statement);
        }
    }

    @Override
    public void downgradeSchema(SQLiteDatabase database) {

    }
}
