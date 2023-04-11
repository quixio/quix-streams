from ..native.Python.QuixStreamsStreaming.Utils.CodecSettings import CodecSettings as csi
from ..helpers.enumconverter import EnumConverter as ec
from ..models import CodecType


class CodecSettings(object):
    """
    Global Codec settings for streams.
    """

    @staticmethod
    def set_global_codec_type(codec_type: CodecType):
        """
        Sets the codec type to be used by producers and transfer package value serialization.
        """
        net_enum = ec.enum_to_another(codec_type, CodecType)
        csi.SetGlobalCodecType(net_enum)

