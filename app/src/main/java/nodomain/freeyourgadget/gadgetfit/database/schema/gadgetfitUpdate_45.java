/*  Copyright (C) 2022-2024 José Rebelo

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
import nodomain.freeyourgadget.gadgetfit.entities.WorldClockDao;

public class gadgetfitUpdate_45 implements DBUpdateScript {
    @Override
    public void upgradeSchema(final SQLiteDatabase db) {
        if (!DBHelper.existsColumn(WorldClockDao.TABLENAME, WorldClockDao.Properties.Code.columnName, db)) {
            final String statement = "ALTER TABLE " + WorldClockDao.TABLENAME + " ADD COLUMN "
                    + WorldClockDao.Properties.Code.columnName + " TEXT";
            db.execSQL(statement);
        }

        if (!DBHelper.existsColumn(WorldClockDao.TABLENAME, WorldClockDao.Properties.Enabled.columnName, db)) {
            final String statement = "ALTER TABLE " + WorldClockDao.TABLENAME + " ADD COLUMN "
                    + WorldClockDao.Properties.Enabled.columnName + " BOOLEAN DEFAULT TRUE";
            db.execSQL(statement);
        }
    }

    @Override
    public void downgradeSchema(final SQLiteDatabase db) {
    }
}
