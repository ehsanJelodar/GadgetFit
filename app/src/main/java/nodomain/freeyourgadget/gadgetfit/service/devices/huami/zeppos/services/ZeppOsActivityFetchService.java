/*  Copyright (C) 2025 José Rebelo

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
package nodomain.freeyourgadget.gadgetfit.service.devices.huami.zeppos.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nodomain.freeyourgadget.gadgetfit.service.devices.huami.HuamiFetcher;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.zeppos.AbstractZeppOsService;
import nodomain.freeyourgadget.gadgetfit.service.devices.huami.zeppos.ZeppOsSupport;
import nodomain.freeyourgadget.gadgetfit.util.GB;

public class ZeppOsActivityFetchService extends AbstractZeppOsService {
    private static final Logger LOG = LoggerFactory.getLogger(ZeppOsActivityFetchService.class);

    private static final short ENDPOINT = 0x004b;

    private final HuamiFetcher fetcher;

    public ZeppOsActivityFetchService(final ZeppOsSupport support, final HuamiFetcher fetcher) {
        super(support, true);
        this.fetcher = fetcher;
    }

    @Override
    public short getEndpoint() {
        return ENDPOINT;
    }

    @Override
    public void handlePayload(final byte[] payload) {
        LOG.trace("Passing to fetcher: {}", GB.hexdump(payload));
        fetcher.onActivityControl(payload);
    }

    public void writeActivityControl(final String name, final byte[] value) {
        write(name, value);
    }
}
