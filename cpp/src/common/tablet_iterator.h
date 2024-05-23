
#ifndef COMMON_TABLET_H
#define COMMON_TABLET_H

namespace storage {

#define GET_TYPED_NEXT(timestamp, CppType, TsType, value) \
    do {                                                  \
        if (row_iter_ >= max_rows_) {                     \
            return E_NO_MORE_DATA;                        \
        }                                                 \
        if (data_type_ != TsType) {                       \
            return E_DATA_TYPE_NOT_MATCH;                 \
        }                                                 \
        timestamp = tablet_.timestamps_[row_iter_];       \
        void *value_arr = value_matrix_[col_idx_];        \
        if (data_type_ == TsType) {                       \
            value = ((CppType *)value_arr) + row_iter_;   \
        }                                                 \
        return E_OK;                                      \
    } while (false)

class TabletColIterator {
   public:
    TabletColIterator(const Tablet &tablet, int col_idx)
        : tablet_(tablet), col_idx_(col_idx) {
        ASSERT(col_idx <= tablet.schema_vec_->size());
        data_type = get_data_type_size(tablet.schema_vec_->at(i).data_type_);
        row_iter_ = 0;
    }

    const MeasurementSchema &get_measurement_schema() const {
        return schema_vec_->at(col_idx_);
    }

    int get_next(int64_t &timestamp, bool &value) {
        GET_TYPED_NEXT(timestamp, bool, BOOLEAN, value);
    }
    int get_next(int64_t &timestamp, int32_t &value) {
        GET_TYPED_NEXT(timestamp, int32_t, INT32, value);
    }
    int get_next(int64_t &timestamp, int64_t &value) {
        GET_TYPED_NEXT(timestamp, int64_t, INT64, value);
    }
    int get_next(int64_t &timestamp, float &value) {
        GET_TYPED_NEXT(timestamp, float, FLOAT, value);
    }
    int get_next(int64_t &timestamp, double &value) {
        GET_TYPED_NEXT(timestamp, double, DOUBLE, value);
    }

   private:
    const Tablet &tablet_;
    TSDataType data_type_;
    int col_idx_;
    int row_iter_;
};

}  // end namespace storage
#endif
