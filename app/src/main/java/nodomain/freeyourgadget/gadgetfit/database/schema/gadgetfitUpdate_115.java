package nodomain.freeyourgadget.gadgetfit.database.schema;

import android.database.sqlite.SQLiteDatabase;

import nodomain.freeyourgadget.gadgetfit.database.DBHelper;
import nodomain.freeyourgadget.gadgetfit.database.DBUpdateScript;
import nodomain.freeyourgadget.gadgetfit.entities.UserAttributesDao;

public class gadgetfitUpdate_115 implements DBUpdateScript {
    @Override
    public void upgradeSchema(SQLiteDatabase database) {
        final String SLEEP_GOAL_MPD = UserAttributesDao.Properties.SleepGoalMPD.columnName;
        final String SLEEP_GOAL_HPD = UserAttributesDao.Properties.SleepGoalMPD.columnName.replace("HPD", "MPD");
        final String USER_ATTRIBUTES = UserAttributesDao.TABLENAME;

        if (!DBHelper.existsColumn(USER_ATTRIBUTES, SLEEP_GOAL_MPD, database)) {
            final String add = "ALTER TABLE " + USER_ATTRIBUTES + " ADD COLUMN " + SLEEP_GOAL_MPD + " INTEGER;";
            final String update = "UPDATE " + USER_ATTRIBUTES + " SET " + SLEEP_GOAL_MPD + " = 60 * " + SLEEP_GOAL_HPD + ";";
            database.execSQL(add);
            database.execSQL(update);
        }
    }

    @Override
    public void downgradeSchema(SQLiteDatabase db) {
    }
}
