#ifndef COMMON_READ_COMMON_PATH_H
#define COMMON_READ_COMMON_PATH_H

#include <string>

namespace storage {

struct Path {
    std::string measurement_;
    std::string device_;
    std::string full_path_;

    Path() {}

    Path(std::string &device, std::string &measurement)
        : measurement_(measurement), device_(device) {
        full_path_ = device + "." + measurement;
    }

    bool operator==(const Path &path) {
        if (measurement_.compare(path.measurement_) == 0 &&
            device_.compare(path.device_) == 0) {
            return true;
        } else {
            return false;
        }
    }
};

}  // namespace storage

#endif  // COMMON_READ_COMMON_PATH_H
