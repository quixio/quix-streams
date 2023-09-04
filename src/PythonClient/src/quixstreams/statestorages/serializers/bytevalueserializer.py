import io
import pickle
import struct

from quixstreams.statestorages.statetype import StateType
from quixstreams.statestorages.statevalue import StateValue


class ByteValueSerializer:
    # Serializer written using struct module
    # Custom protocol

    CodecId = 'b'

    @staticmethod
    def serialize(state_value: StateValue) -> bytes:
        """Serializes StateValue into byte array"""
        with io.BytesIO() as stream:
            stream.write(struct.pack('c', ByteValueSerializer.CodecId.encode()))
            stream.write(struct.pack('c', state_value.type.value.encode()))

            if state_value.type == StateType.Binary:
                if state_value.value is None:
                    stream.write(struct.pack('>i', -1))
                else:
                    stream.write(struct.pack('>i', len(state_value.value)))
                    stream.write(state_value.value)

            elif state_value.type == StateType.Object:
                if state_value.value is None:
                    stream.write(struct.pack('>i', -1))
                else:
                    value_in_bytes = pickle.dumps(state_value.value)
                    stream.write(struct.pack('>i', len(value_in_bytes)))
                    stream.write(value_in_bytes)

            elif state_value.type == StateType.Bool:
                stream.write(struct.pack('?', state_value.value))

            elif state_value.type == StateType.Double:
                stream.write(struct.pack('>d', state_value.value))

            elif state_value.type == StateType.Long:
                stream.write(struct.pack('>q', state_value.value))

            elif state_value.type == StateType.String:
                stream.write(state_value.value.encode())
            else:
                raise ValueError(f'Unsupported type {state_value.type} in Serialization')

            return stream.getvalue()

    @staticmethod
    def deserialize(data: bytes) -> StateValue:
        """Deserializes byte array into StateValue"""
        with io.BytesIO(data) as stream:
            codec_id = stream.read(1).decode()
            if codec_id != ByteValueSerializer.CodecId:
                raise ValueError('Wrong codec id')

            data_type = stream.read(1).decode()

            if data_type == StateType.Binary.value:
                data_length = struct.unpack('>i', stream.read(4))[0]
                if data_length == -1:
                    return StateValue(None)
                return StateValue(stream.read(data_length))

            elif data_type == StateType.Object.value:
                data_length = struct.unpack('>i', stream.read(4))[0]
                if data_length == -1:
                    return StateValue(None)
                return StateValue(pickle.loads(stream.read(data_length)))

            elif data_type == StateType.Bool.value:
                return StateValue(struct.unpack('?', stream.read(1))[0])

            elif data_type == StateType.Double.value:
                return StateValue(struct.unpack('>d', stream.read(8))[0])

            elif data_type == StateType.Long.value:
                return StateValue(struct.unpack('>q', stream.read(8))[0])

            elif data_type == StateType.String.value:
                return StateValue(stream.read().decode())

            else:
                raise ValueError(f'Unsupported type {data_type} in Serialization')
