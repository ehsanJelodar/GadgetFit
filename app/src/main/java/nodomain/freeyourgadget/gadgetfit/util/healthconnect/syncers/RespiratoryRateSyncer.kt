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

import androidx.health.connect.client.records.RespiratoryRateRecord
import androidx.health.connect.client.records.metadata.Metadata
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice
import nodomain.freeyourgadget.gadgetfit.model.RespiratoryRateSample
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.ZoneOffset
import kotlin.reflect.KClass

internal object RespiratoryRateSyncer : AbstractTimeSampleSyncer<RespiratoryRateSample, RespiratoryRateRecord>() {
    override val logger: Logger = LoggerFactory.getLogger(RespiratoryRateSyncer::class.java)
    override val recordClass: KClass<RespiratoryRateRecord> = RespiratoryRateRecord::class

    override fun getSampleProvider(
        gbDevice: GBDevice,
        daoSession: DaoSession
    ): TimeSampleProvider<out RespiratoryRateSample>? {
        return gbDevice.deviceCoordinator.getRespiratoryRateSampleProvider(gbDevice, daoSession)
    }

    override fun convertSample(
        sample: RespiratoryRateSample,
        offset: ZoneOffset,
        metadata: Metadata,
        deviceName: String
    ): RespiratoryRateRecord? {
        if (sample.respiratoryRate <= 0) {
            return null
        }

        return RespiratoryRateRecord(
            time = Instant.ofEpochMilli(sample.timestamp),
            zoneOffset = offset,
            rate = sample.respiratoryRate.toDouble(),
            metadata = metadata
        )
    }
}
