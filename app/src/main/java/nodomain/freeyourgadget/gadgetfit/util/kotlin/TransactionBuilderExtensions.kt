package nodomain.freeyourgadget.gadgetfit.util.kotlin

import nodomain.freeyourgadget.gadgetfit.service.btle.AbstractBTLESingleDeviceSupport

inline fun AbstractBTLESingleDeviceSupport.withTransaction(
    name: String,
    block: (nodomain.freeyourgadget.gadgetfit.service.btle.TransactionBuilder) -> Unit
) {
    val builder = createTransactionBuilder(name)
    block(builder)
    builder.queue()
}
