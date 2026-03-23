package nodomain.freeyourgadget.gadgetfit.service.devices.garmin.http.interceptors

import nodomain.freeyourgadget.gadgetfit.service.devices.garmin.http.GarminHttpRequest
import nodomain.freeyourgadget.gadgetfit.service.devices.garmin.http.GarminHttpResponse

interface HttpInterceptor {
    fun supports(request: GarminHttpRequest): Boolean
    fun handle(request: GarminHttpRequest): GarminHttpResponse?
}
