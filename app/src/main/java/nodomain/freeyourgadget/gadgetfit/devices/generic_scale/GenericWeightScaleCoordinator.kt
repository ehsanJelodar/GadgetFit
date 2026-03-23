/*  Copyright (C) 2025 Thomas Kuehne

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
package nodomain.freeyourgadget.gadgetfit.devices.generic_scale

import de.greenrobot.dao.AbstractDao
import de.greenrobot.dao.Property
import nodomain.freeyourgadget.gadgetfit.R
import nodomain.freeyourgadget.gadgetfit.devices.AbstractBLEDeviceCoordinator
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCardAction
import nodomain.freeyourgadget.gadgetfit.devices.DeviceCoordinator
import nodomain.freeyourgadget.gadgetfit.devices.GenericWeightSampleProvider
import nodomain.freeyourgadget.gadgetfit.devices.TimeSampleProvider
import nodomain.freeyourgadget.gadgetfit.entities.DaoSession
import nodomain.freeyourgadget.gadgetfit.entities.GenericWeightSampleDao
import nodomain.freeyourgadget.gadgetfit.impl.GBDevice
import nodomain.freeyourgadget.gadgetfit.impl.GBDeviceCandidate
import nodomain.freeyourgadget.gadgetfit.model.WeightSample
import nodomain.freeyourgadget.gadgetfit.service.DeviceSupport
import nodomain.freeyourgadget.gadgetfit.service.btle.GattService
import nodomain.freeyourgadget.gadgetfit.service.devices.generic_scale.GenericWeightScaleSupport
import java.util.Collections

class GenericWeightScaleCoordinator : AbstractBLEDeviceCoordinator() {
    override fun getAllDeviceDao(session: DaoSession): MutableMap<AbstractDao<*, *>?, Property?> {
        return Collections.singletonMap(
            session.genericWeightSampleDao, GenericWeightSampleDao.Properties.DeviceId
        )
    }

    override fun getCustomActions(): MutableList<DeviceCardAction?> {
        return Collections.singletonList<DeviceCardAction>(GenericWeightScaleAction())
    }

    override fun getOrderPriority(): Int {
        return Int.MAX_VALUE
    }

    override fun getSupportedDeviceSpecificSettings(device: GBDevice?): IntArray? {
        return intArrayOf(R.xml.devicesettings_weight_scale_unit)
    }

    override fun getWeightSampleProvider(
        device: GBDevice, session: DaoSession
    ): TimeSampleProvider<out WeightSample?> {
        return GenericWeightSampleProvider(device, session)
    }

    override fun isExperimental(): Boolean {
        // needs more testing
        return true
    }

    override fun getManufacturer(): String? {
        return "Generic"
    }

    override fun getDeviceSupportClass(device: GBDevice?): Class<out DeviceSupport?> {
        return GenericWeightScaleSupport::class.java
    }

    override fun getBondingStyle(): Int {
        return BONDING_STYLE_ASK
    }

    override fun getDefaultIconResource(): Int {
        return R.drawable.ic_device_miscale
    }

    override fun getDeviceNameResource(): Int {
        return R.string.devicetype_generic_weight_scale
    }

    override fun supports(candidate: GBDeviceCandidate): Boolean {
        return candidate.supportsService(GattService.UUID_SERVICE_WEIGHT_SCALE)
    }

    override fun supportsWeightMeasurement(device: GBDevice): Boolean {
        return true
    }

    override fun supportsCharts(device: GBDevice): Boolean {
        return true
    }

    override fun getDeviceKind(device: GBDevice): DeviceCoordinator.DeviceKind {
        return DeviceCoordinator.DeviceKind.SCALE
    }
}