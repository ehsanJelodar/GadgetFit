/*  Copyright (C) 2020-2024 Andreas Shimokawa

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
package nodomain.freeyourgadget.gadgetfit.database.schema;

import android.database.sqlite.SQLiteDatabase;

import nodomain.freeyourgadget.gadgetfit.database.DBHelper;
import nodomain.freeyourgadget.gadgetfit.database.DBUpdateScript;
import nodomain.freeyourgadget.gadgetfit.entities.AlarmDao;

public class gadgetfitUpdate_26 implements DBUpdateScript {
    @Override
    public void upgradeSchema(SQLiteDatabase db) {
        if (!DBHelper.existsColumn(AlarmDao.TABLENAME, AlarmDao.Properties.Title.columnName, db)) {
            String ADD_COLUMN_TITLE = "ALTER TABLE " + AlarmDao.TABLENAME + " ADD COLUMN "
                    + AlarmDao.Properties.Title.columnName + " TEXT";
            db.execSQL(ADD_COLUMN_TITLE);
        }
        if (!DBHelper.existsColumn(AlarmDao.TABLENAME, AlarmDao.Properties.Description.columnName, db)) {
            String ADD_COLUMN_TITLE = "ALTER TABLE " + AlarmDao.TABLENAME + " ADD COLUMN "
                    + AlarmDao.Properties.Description.columnName + " TEXT";
            db.execSQL(ADD_COLUMN_TITLE);
        }
    }

    @Override
    public void downgradeSchema(SQLiteDatabase db) {
    }
}
