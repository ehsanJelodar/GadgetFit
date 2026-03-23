/*  Copyright (C) 2026 José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.util.healthconnect.syncers

import androidx.health.connect.client.records.BloodGlucoseRecord
import androidx.health.connect.client.records.metadata.Metadata
import androidx.health.connect.client.units.BloodGlucose
import nodomain.freeyourgadget.gadgetfit.devices.GlucoseSampleProvider
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession
import nodomain.freeyourgadget.gadgetfit.entities.GlucoseSample
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.ZoneOffset
import kotlin.reflect.KClass

internal object BloodGlucoseSyncer : AbstractTimeSampleSyncer<GlucoseSample, BloodGlucoseRecord>() {
    override val logger: Logger = LoggerFactory.getLogger(BloodGlucoseSyncer::class.java)
    override val recordClass: KClass<BloodGlucoseRecord> = BloodGlucoseRecord::class

    override fun getSampleProvider(
        gbDevice: GBDevice,
        daoSession: DaoSession
    ): TimeSampleProvider<out GlucoseSample> {
        @Suppress("UNCHECKED_CAST")
        return GlucoseSampleProvider(gbDevice, daoSession) as TimeSampleProvider<out GlucoseSample>
    }

    override fun convertSample(
        sample: GlucoseSample,
        offset: ZoneOffset,
        metadata: Metadata,
        deviceName: String
    ): BloodGlucoseRecord {
        return BloodGlucoseRecord(
            time = Instant.ofEpochMilli(sample.timestamp),
            zoneOffset = offset,
            level = BloodGlucose.milligramsPerDeciliter(sample.valueMgDl),
            metadata = metadata
        )
    }
}
