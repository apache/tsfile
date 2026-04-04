//
// Created by 李烁麟 on 25-8-4.
//

#ifndef UTILS_H
#define UTILS_H
#include <iostream>

static uint32_t read_ui32(uint8_t*& data) {
    uint8_t buf[4];
    memcpy(buf, data, 4);
    data += 4;
    uint32_t ui32 = buf[0];
    ui32 = (ui32 << 8) | (buf[1] & 0xFF);
    ui32 = (ui32 << 8) | (buf[2] & 0xFF);
    ui32 = (ui32 << 8) | (buf[3] & 0xFF);
    return ui32;
}

static uint64_t read_ui64(uint8_t*& data) {
    uint8_t buf[8];
    memcpy(buf, data, 8);
    data += 8;
    uint64_t ui64 = buf[0];
    for (int i = 1; i < 8; ++i) {
        ui64 = (ui64 << 8) | (buf[i] & 0xFF);
    }
    return ui64;
}

static void write_uint32(const uint32_t ui32, uint8_t*& value) {
    uint8_t buf[4];
    buf[0] = static_cast<uint8_t>((ui32 >> 24) & 0xFF);
    buf[1] = static_cast<uint8_t>((ui32 >> 16) & 0xFF);
    buf[2] = static_cast<uint8_t>((ui32 >> 8) & 0xFF);
    buf[3] = static_cast<uint8_t>((ui32) & 0xFF);
    memcpy(value, buf, 4);
    value += 4;
}

static void write_uint64(const uint64_t ui64, uint8_t*& value) {
    uint8_t buf[8];
    buf[0] = static_cast<uint8_t>((ui64 >> 56) & 0xFF);
    buf[1] = static_cast<uint8_t>((ui64 >> 48) & 0xFF);
    buf[2] = static_cast<uint8_t>((ui64 >> 40) & 0xFF);
    buf[3] = static_cast<uint8_t>((ui64 >> 32) & 0xFF);
    buf[4] = static_cast<uint8_t>((ui64 >> 24) & 0xFF);
    buf[5] = static_cast<uint8_t>((ui64 >> 16) & 0xFF);
    buf[6] = static_cast<uint8_t>((ui64 >> 8) & 0xFF);
    buf[7] = static_cast<uint8_t>((ui64) & 0xFF);
    memcpy(value, buf, 8);
    value += 8;
}

#endif  // UTILS_H
