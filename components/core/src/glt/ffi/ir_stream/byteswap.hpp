#ifndef CLP_FFI_IR_STREAM_BYTESWAP_HPP
#define CLP_FFI_IR_STREAM_BYTESWAP_HPP

#ifdef __APPLE__
    #include <libkern/OSByteOrder.h>
    #define bswap_16(x) OSSwapInt16(x)
    #define bswap_32(x) OSSwapInt32(x)
    #define bswap_64(x) OSSwapInt64(x)
#else
    #include <byteswap.h>
#endif

#endif  // CLP_FFI_IR_STREAM_BYTESWAP_HPP
