import io

import avro.schema
from avro.io import DatumWriter, BinaryEncoder, BinaryDecoder, DatumReader

schema = avro.schema.parse(open("../schemas/trade.avsc").read())


def trade_serialize(trade):
    buffer = io.BytesIO()
    encoder = BinaryEncoder(buffer)
    writer = DatumWriter(schema)

    writer.write(trade, encoder)

    data = buffer.getvalue()
    return data


def trade_deserialize(data):
    buffer = io.BytesIO(data)
    decoder = BinaryDecoder(buffer)
    reader = DatumReader(schema)

    decoded = reader.read(decoder)
    return decoded
