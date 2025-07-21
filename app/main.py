import logging
import time

from make87_messages.text.text_plain_pb2 import PlainText
from make87_messages.core.header_pb2 import Header
from make87.encodings import ProtobufEncoder
from make87.interfaces.zenoh import ZenohInterface

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    format="[%(asctime)sZ %(levelname)s  %(name)s] %(message)s", level=logging.INFO, datefmt="%Y-%m-%dT%H:%M:%S"
)


def main():
    message_encoder = ProtobufEncoder(message_type=PlainText)
    zenoh_interface = ZenohInterface(name="zenoh")

    queryable = zenoh_interface.get_queryable("message_endpoint")

    while True:
        with queryable.recv() as query:
            message = message_encoder.decode(query.payload.to_bytes())
            logging.info(f"Received: '{message}'")

            header = Header()
            header.CopyFrom(message.header)
            header.timestamp.GetCurrentTime()

            response_message = PlainText(
                header=header,
                body=message.body[::-1],  # Reverse the message body for demonstration
            )
            message_encoded = message_encoder.encode(response_message)

            query.reply(key_expr=query.key_expr, payload=message_encoded)


if __name__ == "__main__":
    main()
