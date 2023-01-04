# Message compression

The Quix SDK uses efficient `ParameterData` transport objects to transmit messages through the message broker, reducing them by an average factor of 10 times compared with plain JSON conversion.

In addition, we apply codecs like [Protobuf](https://en.wikipedia.org/wiki/Protocol_Buffers) and [_gzip_](https://www.gzip.org/) compression to achieve the best performance possible with the minimum payload.

![Compression ratios after Protobuf + Gzip](../images/QuixCompression.png)

All these improvements are completely transparent and you don’t even need to worry about them because everything is implemented for you in the SDK.
