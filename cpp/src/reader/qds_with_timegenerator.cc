/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "qds_with_timegenerator.h"

#include "reader/filter/time_filter.h"
#include "reader/filter/time_operator.h"

using namespace common;

namespace storage {

int SeriesScanStream::init() {
    int ret = E_OK;
    ASSERT(tsblock_ == nullptr);
    if (RET_FAIL(ssi_->get_next(tsblock_, true))) {
    } else {
        col_iter_ = new common::ColIterator(0, tsblock_);  // 0 for time_col
    }
    return ret;
}

void SeriesScanStream::destroy() {
    if (col_iter_ != nullptr) {
        delete col_iter_;
        col_iter_ = nullptr;
    }
    if (ssi_ != nullptr && tsblock_ != nullptr) {
        ssi_->revert_tsblock();
        tsblock_ = nullptr;
    }
    if (ssi_ != nullptr) {
        io_reader_->revert_ssi(ssi_);
        ssi_ = nullptr;
    }
}

int64_t SeriesScanStream::front() {
    if (col_iter_ == nullptr) {
        return INVALID_NEXT_TIMESTAMP;
    } else {
        if (col_iter_->end()) {
            int ret = E_OK;  // cppcheck-suppress unreadVariable
            if (RET_FAIL(ssi_->get_next(tsblock_, false))) {
                delete col_iter_;
                col_iter_ = nullptr;
                return INVALID_NEXT_TIMESTAMP;
            } else {
                delete col_iter_;
                col_iter_ = new ColIterator(0, tsblock_);
                ASSERT(!col_iter_->end());
            }
        }
        return read_timestamp();
    }
}

// stop after @beyond_this_time
void SeriesScanStream::pop_front(int64_t beyond_this_time) {
    /*
     * advance col_iter_ after @beyond_this_time
     * if current tsblock reach end, go to next tsblock.
     */
    while (true) {
        int64_t cur_front = front();
        if (cur_front == INVALID_NEXT_TIMESTAMP) {
            return;
        }
        if (cur_front > beyond_this_time) {
            return;
        }
        // @cur_front is valid timestamp, it mean @col_iter_ is still valid now.
        col_iter_->next();
    }
}

int64_t SeriesScanStream::read_timestamp() {
    uint32_t ret_len = 0;
    bool is_null = false;
    char *data = col_iter_->read(&ret_len, &is_null);
    ASSERT(ret_len == 8);
    return *(int64_t *)data;
}

// get value object pointer at time @target_timestamp
// if no such TV exists, return nullptr
void *ValueAt::at(int64_t target_timestamp) {
    ASSERT(ssi_ != nullptr);
    if (cur_time_ > target_timestamp) {
        return nullptr;
    }
    int ret = common::E_OK;
    if (time_col_iter_ == nullptr) {
        tf_ = TimeFilter::gt_eq(target_timestamp);
        if (RET_FAIL(ssi_->get_next(tsblock_, (tsblock_ == nullptr), tf_))) {
            cur_time_ = INT64_MAX;
            return nullptr;
        }
        data_type_ = tsblock_->get_tuple_desc()->get_column_desc(1).type_;
        time_col_iter_ = new ColIterator(0, tsblock_);
        value_col_iter_ = new ColIterator(1, tsblock_);
    }

    uint32_t ret_len = 0;
    while (true) {
        while (!time_col_iter_->end()) {
            char *iter_time_ptr = time_col_iter_->read(&ret_len);
            cur_time_ = *(int64_t *)iter_time_ptr;
            if (cur_time_ == target_timestamp) {
                char *val_obj_ptr = value_col_iter_->read(&ret_len);
                time_col_iter_->next();
                value_col_iter_->next();
                return val_obj_ptr;
            } else if (cur_time_ < target_timestamp) {
                time_col_iter_->next();
                value_col_iter_->next();
                continue;
            } else {
                return nullptr;
            }
        }
        tf_->reset_value(target_timestamp);
        if (RET_FAIL(ssi_->get_next(tsblock_, (tsblock_ == nullptr), tf_))) {
            cur_time_ = INT64_MAX;
            return nullptr;
        } else {
            delete time_col_iter_;
            delete value_col_iter_;
            time_col_iter_ = new ColIterator(0, tsblock_);
            value_col_iter_ = new ColIterator(1, tsblock_);
        }
    }
}

void ValueAt::destroy() {
    if (tf_ != nullptr) {
        delete tf_;
        tf_ = nullptr;
    }
    if (time_col_iter_ != nullptr) {
        delete time_col_iter_;
        time_col_iter_ = nullptr;
    }
    if (value_col_iter_ != nullptr) {
        delete value_col_iter_;
        value_col_iter_ = nullptr;
    }
    if (ssi_ != nullptr && tsblock_ != nullptr) {
        ssi_->revert_tsblock();
        tsblock_ = nullptr;
    }
    if (ssi_ != nullptr && io_reader_ != nullptr) {
        io_reader_->revert_ssi(ssi_);
        ssi_ = nullptr;
    }
}

#ifdef DEBUG_SE
int depth = 0;
struct DG {
    explicit DG(int &depth) : depth_(depth) { depth_++; }
    ~DG() { depth_--; }
    std::string get_indent() {
        std::string s;
        for (int i = 0; i < depth_; i++) {
            s = s + "--->";
        }
        return s;
    }
    int &depth_;
};
#endif

// if not exist, return INVALID_NEXT_TIMESTAMP
int64_t Node::get_cur_timestamp() {
#ifdef DEBUG_SE
    DG dg(depth);
#endif
    if (type_ == AND_NODE) {
        while (true) {
            int64_t lt = left_->get_cur_timestamp();
            int64_t rt = right_->get_cur_timestamp();
#if DEBUG_SE
            std::cout << dg.get_indent()
                      << "Node::get_cur_timestamp: AND_NODE, lt=" << lt
                      << ", rt=" << rt << std::endl;
#endif
            if (lt == INVALID_NEXT_TIMESTAMP || rt == INVALID_NEXT_TIMESTAMP) {
                return INVALID_NEXT_TIMESTAMP;
            } else if (lt == rt) {
                return lt;
            } else if (lt < rt) {
                left_->next_timestamp(rt - 1);
            } else {
                right_->next_timestamp(lt - 1);
            }
        }
    } else if (type_ == OR_NODE) {
        while (true) {
            int64_t lt = left_->get_cur_timestamp();
            int64_t rt = right_->get_cur_timestamp();
#if DEBUG_SE
            std::cout << dg.get_indent()
                      << "Node::get_cur_timestamp:  OR_NODE, lt=" << lt
                      << ", rt=" << rt << std::endl;
#endif
            if (lt == INVALID_NEXT_TIMESTAMP && rt == INVALID_NEXT_TIMESTAMP) {
                next_direction_ = STOP_NEXT;
                return INVALID_NEXT_TIMESTAMP;
            } else if (lt == INVALID_NEXT_TIMESTAMP) {
                next_direction_ = RIGHT_NEXT;
                return rt;
            } else if (rt == INVALID_NEXT_TIMESTAMP) {
                next_direction_ = LEFT_NEXT;
                return lt;
            } else if (lt == rt) {
                next_direction_ = BOTH_NEXT;
                return lt;
            } else if (lt < rt) {
                next_direction_ = LEFT_NEXT;
                return lt;
            } else {
                next_direction_ = RIGHT_NEXT;
                return rt;
            }
        }
    } else {
        int64_t res = sss_.front();
#if DEBUG_SE
        std::cout << dg.get_indent()
                  << "Node::get_cur_timestamp: SRS_NODE, res=" << res
                  << std::endl;
#endif
        return res;
    }
}

// after calling @next_timestamp, @get_cur_timestamp will return next timestamp
void Node::next_timestamp(int64_t beyond_this_time) {
    if (type_ == AND_NODE) {
#if DEBUG_SE
        std::cout << "next_timestamp. AND_NODE. beyond_this_time="
                  << beyond_this_time << std::endl;
#endif
        left_->next_timestamp(beyond_this_time);
        right_->next_timestamp(beyond_this_time);
    } else if (type_ == OR_NODE) {
#if DEBUG_SE
        std::cout << "next_timestamp.  OR_NODE. beyond_this_time="
                  << beyond_this_time << ", next_direction_=" << next_direction_
                  << std::endl;
#endif
        if (next_direction_ == STOP_NEXT) {
            // do nothing
        } else if (next_direction_ == BOTH_NEXT) {
            left_->next_timestamp(beyond_this_time);
            right_->next_timestamp(beyond_this_time);
        } else if (next_direction_ == LEFT_NEXT) {
            left_->next_timestamp(beyond_this_time);
        } else if (next_direction_ == RIGHT_NEXT) {
            right_->next_timestamp(beyond_this_time);
        } else {
            ASSERT(false);
        }
    } else {  // SERIES
        sss_.pop_front(beyond_this_time);
    }
}

int QDSWithTimeGenerator::init(TsFileIOReader *io_reader, QueryExpression *qe) {
    int ret = common::E_OK;  // cppcheck-suppress unreadVariable
    io_reader_ = io_reader;
    qe_ = qe;
    std::vector<Path> paths = qe_->selected_series_;

    for (size_t i = 0; i < paths.size(); i++) {
        ValueAt va;
        if (RET_FAIL(io_reader_->alloc_ssi(paths[i].device_,
                                           paths[i].measurement_, va.ssi_))) {
        } else {
            va.io_reader_ = io_reader_;
            value_at_vec_.push_back(va);
        }
    }

    row_record_ = new RowRecord(value_at_vec_.size());
    tree_ = construct_node_tree(qe->expression_);
    return E_OK;
}

void destroy_node(Node *node) {
    if (node->left_) {
        destroy_node(node->left_);
    }
    if (node->right_) {
        destroy_node(node->right_);
    }
    delete node;
}

void QDSWithTimeGenerator::destroy() {
    if (row_record_ != nullptr) {
        delete row_record_;
        row_record_ = nullptr;
    }
    if (tree_ != nullptr) {
        destroy_node(tree_);
        tree_ = nullptr;
    }
    for (size_t i = 0; i < value_at_vec_.size(); i++) {
        value_at_vec_[i].destroy();
    }
    value_at_vec_.clear();
}

RowRecord *QDSWithTimeGenerator::get_next() {
    if (tree_ == nullptr) {
        return nullptr;
    }
    int64_t timestamp = tree_->get_cur_timestamp();
    if (timestamp == INVALID_NEXT_TIMESTAMP) {
        return nullptr;
    }
    row_record_->set_timestamp(timestamp);
#if DEBUG_SE
    std::cout << "QDSWithTimeGenerator::get_next: timestamp=" << timestamp
              << ", will generate row at this timestamp." << std::endl;
#endif

    for (size_t i = 0; i < value_at_vec_.size(); i++) {
        ValueAt &va = value_at_vec_[i];
        void *val_obj_ptr = va.at(timestamp);
        row_record_->get_field(i)->set_value(va.data_type_, val_obj_ptr);
    }

    tree_->next_timestamp(timestamp);
#if DEBUG_SE
    std::cout << "\n\n" << std::endl;
#endif
    return row_record_;
}

Node *QDSWithTimeGenerator::construct_node_tree(Expression *expr) {
    if (expr->type_ == AND_EXPR || expr->type_ == OR_EXPR) {
        Node *root = nullptr;
        if (expr->type_ == AND_EXPR) {
            root = new Node(AND_NODE);
        } else {
            root = new Node(OR_NODE);
        }
        root->left_ = construct_node_tree(expr->left_);
        root->right_ = construct_node_tree(expr->right_);
        return root;
    } else if (expr->type_ == SERIES_EXPR) {
        Node *leaf = new Node(LEAF_NODE);
        Path &path = expr->series_path_;
        int ret = io_reader_->alloc_ssi(path.device_, path.measurement_,
                                        leaf->sss_.ssi_, expr->filter_);
        if (E_OK == ret) {
            leaf->sss_.init();
        } else {
            // do nothing, this leaf node will return no data at all.
        }
        return leaf;
    }
    return nullptr;
}

}  // namespace storage
