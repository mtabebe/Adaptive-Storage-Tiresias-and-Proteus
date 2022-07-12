#pragma once

#include <vector>
#include <unordered_map>

#include "record-types/tpcc_primary_key_generation.h"
#include "record-types/tpcc_record_types.h"
#include "tpcc_configs.h"
#include "tpcc_table_ids.h"

#define EmptyProbe uint64_t
#define emptyProbeVal 0

class q1_agg {
   public:
    q1_agg();

    void reduce( const q1_agg& val );

    int32_t quantity_count_;
    float quantity_sum_;

    int32_t amount_count_;
    float    amount_sum_;
};

#define q1_kv_types                                          \
    int32_t /* Key */, q1_agg /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q1_kv_vec_types                                                   \
    int32_t /* Key */, std::vector<q1_agg> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q1_mapper( const std::vector<result_tuple>& res_tuples,
                std::unordered_map<q1_kv_types>& res, const EmptyProbe& p );

void q1_reducer( const std::unordered_map<q1_kv_vec_types>& input,
                 std::unordered_map<q1_kv_types>& res, const EmptyProbe& p );

#define q1_types q1_kv_types, EmptyProbe, q1_mapper, q1_reducer

#define q4_order_kv_types                                     \
    uint64_t /* Key */, order /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q4_order_kv_vec_types                                              \
    uint64_t /* Key */, std::vector<order> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

void q4_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q4_order_kv_types>& res,
                      const EmptyProbe&                      p );

void q4_order_reducer( const std::unordered_map<q4_order_kv_vec_types>& input,
                       std::unordered_map<q4_order_kv_types>&           res,
                       const EmptyProbe&                                p );

#define q4_order_types \
    q4_order_kv_types, EmptyProbe, q4_order_mapper, q4_order_reducer

#define q4_order_line_kv_types                                   \
    uint64_t /* Key */, uint64_t /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q4_order_line_kv_vec_types                                            \
    uint64_t /* Key */, std::vector<uint64_t> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

class q4_order_line_probe {
   public:
    q4_order_line_probe( const std::unordered_map<q4_order_kv_types>& probe,
                         const tpcc_configs&                          cfg );

    const std::unordered_map<q4_order_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

void q4_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q4_order_line_kv_types>& res,
                           const q4_order_line_probe&                  probe );
void q4_order_line_reducer(
    const std::unordered_map<q4_order_line_kv_vec_types>& input,
    std::unordered_map<q4_order_line_kv_types>&           res,
    const q4_order_line_probe&                            probe );

#define q4_order_line_types                                            \
    q4_order_line_kv_types, q4_order_line_probe, q4_order_line_mapper, \
        q4_order_line_reducer

#define q6_kv_types                                         \
    int32_t /* Key */, float /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q6_kv_vec_types                                                  \
    int32_t /* Key */, std::vector<float> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q6_mapper( const std::vector<result_tuple>& res_tuples,
                std::unordered_map<q6_kv_types>& res, const EmptyProbe& p );

void q6_reducer( const std::unordered_map<q6_kv_vec_types>& input,
                 std::unordered_map<q6_kv_types>& res, const EmptyProbe& p );

#define q6_types q6_kv_types, EmptyProbe, q6_mapper, q6_reducer

#define q14_item_kv_types                                  \
    int32_t /* Key */, item /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q14_item_kv_vec_types                                          \
    int32_t /* Key */, std::vector<item> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q14_item_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q14_item_kv_types>& res,
                       const EmptyProbe&                       p );

void q14_item_reducer( const std::unordered_map<q14_item_kv_vec_types>& input,
                       std::unordered_map<q14_item_kv_types>&           res,
                       const EmptyProbe&                                 p );

#define q14_item_types \
    q14_item_kv_types, EmptyProbe, q14_item_mapper, q14_item_reducer

#define q14_order_line_kv_types                                  \
    int32_t /* Key */, order_line /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q14_order_line_kv_vec_types                                           \
    int32_t /* Key */, std::vector<order_line> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

class q14_order_line_probe {
   public:
    q14_order_line_probe( const std::unordered_map<q14_item_kv_types>& probe,
                          const tpcc_configs&                          cfg );

    const std::unordered_map<q14_item_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

void q14_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q14_order_line_kv_types>& res,
                            const q14_order_line_probe&                  probe );
void q14_order_line_reducer(
    const std::unordered_map<q14_order_line_kv_vec_types>& input,
    std::unordered_map<q14_order_line_kv_types>&           res,
    const q14_order_line_probe&                            probe );

#define q14_order_line_types                                              \
    q14_order_line_kv_types, q14_order_line_probe, q14_order_line_mapper, \
        q14_order_line_reducer

double compute_q14_value( const item& i, const order_line& ol );

#define q5_order_line_kv_types                                           \
    uint64_t /* Key stock key */, float /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q5_order_line_kv_vec_types                                         \
    uint64_t /* Key */, std::vector<float> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

void q5_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q5_order_line_kv_types>& res,
                           const tpcc_configs&                         p );

void q5_order_line_reducer(
    const std::unordered_map<q5_order_line_kv_vec_types>& input,
    std::unordered_map<q5_order_line_kv_types>& res, const tpcc_configs& p );

#define q5_order_line_types                                     \
    q5_order_line_kv_types, tpcc_configs, q5_order_line_mapper, \
        q5_order_line_reducer

#define q5_nation_kv_types                                             \
    int32_t /* Key nation id */, nation /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q5_nation_kv_vec_types                                            \
    int32_t /* Key */, std::vector<nation> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q5_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q5_nation_kv_types>& res,
                       const EmptyProbe&                       p );

void q5_nation_reducer( const std::unordered_map<q5_nation_kv_vec_types>& input,
                        std::unordered_map<q5_nation_kv_types>&           res,
                        const EmptyProbe&                                 p );

#define q5_nation_types \
    q5_nation_kv_types, EmptyProbe, q5_nation_mapper, q5_nation_reducer

class q5_supplier_probe {
   public:
    q5_supplier_probe( const std::unordered_map<q5_nation_kv_types>& probe,
                       const tpcc_configs&                             cfg );

    const std::unordered_map<q5_nation_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

#define q5_supplier_kv_types                                           \
    int32_t /* Key supplier id */, int32_t /* Val don't need it */, \
        std::hash<int32_t>, std::equal_to<int32_t>
#define q5_supplier_kv_vec_types                                           \
    int32_t /* Key */, std::vector<int32_t> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q5_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q5_supplier_kv_types>& res,
                         const q5_supplier_probe&                  p );

void q5_supplier_reducer(
    const std::unordered_map<q5_supplier_kv_vec_types>& input,
    std::unordered_map<q5_supplier_kv_types>& res, const q5_supplier_probe& p );

#define q5_supplier_types                                        \
    q5_supplier_kv_types, q5_supplier_probe, q5_supplier_mapper, \
        q5_supplier_reducer

class q5_stock_probe {
   public:
    q5_stock_probe( const std::unordered_map<q5_supplier_kv_types>&   s_probe,
                    const std::unordered_map<q5_order_line_kv_types>& ol_probe,
                    const tpcc_configs&                               cfg );

    const std::unordered_map<q5_supplier_kv_types>&   s_probe_;
    const std::unordered_map<q5_order_line_kv_types>& ol_probe_;
    const tpcc_configs&                               cfg_;
};

#define q5_stock_kv_types                                 \
    uint64_t /* stock key */, stock, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q5_stock_kv_vec_types                                              \
    uint64_t /* Key */, std::vector<stock> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

void q5_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q5_stock_kv_types>& res,
                      const q5_stock_probe&                  p );
void q5_stock_reducer( const std::unordered_map<q5_stock_kv_vec_types>& input,
                       std::unordered_map<q5_stock_kv_types>&           res,
                       const q5_stock_probe&                            p );

#define q5_stock_types \
    q5_stock_kv_types, q5_stock_probe, q5_stock_mapper, q5_stock_reducer

#define q3_customer_kv_types                                   \
    uint64_t /* customer key */, int32_t /* ignore the val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q3_customer_kv_vec_types                                             \
    uint64_t /* Key */, std::vector<int32_t> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

void q3_customer_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q3_customer_kv_types>& res,
                         const tpcc_configs&                       p );
void q3_customer_reducer(
    const std::unordered_map<q3_customer_kv_vec_types>& input,
    std::unordered_map<q3_customer_kv_types>& res, const tpcc_configs& p );

#define q3_customer_types \
    q3_customer_kv_types, tpcc_configs, q3_customer_mapper, q3_customer_reducer

#define q3_order_kv_types                                                   \
    uint64_t /* order key */, order /* o entry d*/, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q3_order_kv_vec_types                                              \
    uint64_t /* Key */, std::vector<order> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

class q3_order_probe {
   public:
    q3_order_probe( const std::unordered_map<q3_customer_kv_types>& probe,
                    const tpcc_configs&                             cfg );

    const std::unordered_map<q3_customer_kv_types>& probe_;
    const tpcc_configs&                             cfg_;
};

void q3_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q3_order_kv_types>& res,
                      const q3_order_probe&                  p );
void q3_order_reducer( const std::unordered_map<q3_order_kv_vec_types>& input,
                       std::unordered_map<q3_order_kv_types>&           res,
                       const q3_order_probe&                            p );

#define q3_order_types \
    q3_order_kv_types, q3_order_probe, q3_order_mapper, q3_order_reducer

#define q3_order_line_kv_types                                 \
    uint64_t /* order key */, order_line, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q3_order_line_kv_vec_types                         \
    uint64_t /* Key */, std::vector<order_line> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q3_order_line_probe {
   public:
    q3_order_line_probe( const std::unordered_map<q3_order_kv_types>& probe,
                         const tpcc_configs&                          cfg );

    const std::unordered_map<q3_order_kv_types>&    probe_;
    const tpcc_configs&                             cfg_;
};

void q3_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q3_order_line_kv_types>& res,
                           const q3_order_line_probe&                  p );
void q3_order_line_reducer(
    const std::unordered_map<q3_order_line_kv_vec_types>& input,
    std::unordered_map<q3_order_line_kv_types>&           res,
    const q3_order_line_probe&                            p );

#define q3_order_line_types                                            \
    q3_order_line_kv_types, q3_order_line_probe, q3_order_line_mapper, \
        q3_order_line_reducer

#define q10_nation_kv_types                                             \
    int32_t /* Key nation id */, nation /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q10_nation_kv_vec_types                                            \
    int32_t /* Key */, std::vector<nation> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q10_nation_mapper( const std::vector<result_tuple>&         res_tuples,
                        std::unordered_map<q10_nation_kv_types>& res,
                        const EmptyProbe&                        p );
void q10_nation_reducer(
    const std::unordered_map<q10_nation_kv_vec_types>& input,
    std::unordered_map<q10_nation_kv_types>& res, const EmptyProbe& p );

#define q10_nation_types \
    q10_nation_kv_types, EmptyProbe, q10_nation_mapper, q10_nation_reducer

#define q10_customer_kv_types                                                \
    uint64_t /* Key customer id */, customer /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q10_customer_kv_vec_types                                 \
    uint64_t /* Key customer */, std::vector<customer> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q10_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q10_customer_kv_types>& res,
                          const EmptyProbe&                          p );
void q10_customer_reducer(
    const std::unordered_map<q10_customer_kv_vec_types>& input,
    std::unordered_map<q10_customer_kv_types>& res, const EmptyProbe& p );

#define q10_customer_types \
    q10_customer_kv_types, EmptyProbe, q10_customer_mapper, q10_customer_reducer

class q10_order_probe {
   public:
    q10_order_probe( const std::unordered_map<q10_customer_kv_types>& probe,
                     const tpcc_configs&                              cfg );

    const std::unordered_map<q10_customer_kv_types>& probe_;
    const tpcc_configs&                              cfg_;
};

#define q10_order_kv_types                                               \
    uint64_t /* Key order id */,                                         \
        std::tuple<uint64_t, uint64_t> /* Val customer id, o_entry_d */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q10_order_kv_vec_types                                                \
    uint64_t /* Key order */,                                                 \
        std::vector<                                                          \
            std::tuple<uint64_t, uint64_t>> /* Val customer id, o_entry_d */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q10_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q10_order_kv_types>& res,
                       const q10_order_probe&                  p );
void q10_order_reducer(
    const std::unordered_map<q10_order_kv_vec_types>& input,
    std::unordered_map<q10_order_kv_types>& res, const q10_order_probe& p );

#define q10_order_types \
    q10_order_kv_types, q10_order_probe, q10_order_mapper, q10_order_reducer

class q10_order_line_probe {
   public:
    q10_order_line_probe( const std::unordered_map<q10_order_kv_types>& probe,
                          const tpcc_configs&                           cfg );

    const std::unordered_map<q10_order_kv_types>& probe_;
    const tpcc_configs&                           cfg_;
};

#define q10_order_line_kv_types                             \
    uint64_t /* Key order id */, float /* Val ol_amount */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q10_order_line_kv_vec_types                                   \
    uint64_t /* Key order */, std::vector<float> /* Val ol_amount */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q10_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q10_order_line_kv_types>& res,
                            const q10_order_line_probe&                  p );
void q10_order_line_reducer(
    const std::unordered_map<q10_order_line_kv_vec_types>& input,
    std::unordered_map<q10_order_line_kv_types>&           res,
    const q10_order_line_probe&                            p );

#define q10_order_line_types                                              \
    q10_order_line_kv_types, q10_order_line_probe, q10_order_line_mapper, \
        q10_order_line_reducer

#define q18_customer_kv_types                             \
    uint64_t /* Key customer id */, customer /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q18_customer_kv_vec_types                                           \
    uint64_t /* Key customer */, std::vector<customer> /* Val ol_amount */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q18_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q18_customer_kv_types>& res,
                          const EmptyProbe&                          p );
void q18_customer_reducer(
    const std::unordered_map<q18_customer_kv_vec_types>& input,
    std::unordered_map<q18_customer_kv_types>& res, const EmptyProbe& p );

#define q18_customer_types \
    q18_customer_kv_types, EmptyProbe, q18_customer_mapper, q18_customer_reducer

class q18_order_probe {
   public:
    q18_order_probe( const std::unordered_map<q18_customer_kv_types>& probe,
                     const tpcc_configs&                              cfg );

    const std::unordered_map<q18_customer_kv_types>& probe_;
    const tpcc_configs&                              cfg_;
};

#define q18_order_kv_types                                 \
    uint64_t /* Key order id */,                           \
        std::tuple<uint64_t /*cust id*/, order> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q18_order_kv_vec_types                              \
    uint64_t /* Key order */,                               \
        std::vector<std::tuple<uint64_t, order>> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q18_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q18_order_kv_types>& res,
                       const q18_order_probe&                  p );
void q18_order_reducer( const std::unordered_map<q18_order_kv_vec_types>& input,
                        std::unordered_map<q18_order_kv_types>&           res,
                        const q18_order_probe&                            p );

#define q18_order_types \
    q18_order_kv_types, q18_order_probe, q18_order_mapper, q18_order_reducer

class q18_order_line_probe {
   public:
    q18_order_line_probe( const std::unordered_map<q18_order_kv_types>& probe,
                          const tpcc_configs&                           cfg );

    const std::unordered_map<q18_order_kv_types>&    probe_;
    const tpcc_configs&                              cfg_;
};

#define q18_order_line_kv_types                                  \
    uint64_t /* Key order id */, float /* Val ol_amount */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q18_order_line_kv_vec_types                         \
    uint64_t /* Key order */, std::vector<float> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q18_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q18_order_line_kv_types>& res,
                            const q18_order_line_probe&                  p );
void q18_order_line_reducer(
    const std::unordered_map<q18_order_line_kv_vec_types>& input,
    std::unordered_map<q18_order_line_kv_types>&           res,
    const q18_order_line_probe&                            p );

#define q18_order_line_types                                              \
    q18_order_line_kv_types, q18_order_line_probe, q18_order_line_mapper, \
        q18_order_line_reducer

#define q12_order_kv_types                                             \
    uint64_t /* Key order id */, order /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q12_order_kv_vec_types                              \
    uint64_t /* Key order */, std::vector<order> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q12_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q12_order_kv_types>& res,
                       const tpcc_configs& cfg);
void q12_order_reducer( const std::unordered_map<q12_order_kv_vec_types>& input,
                        std::unordered_map<q12_order_kv_types>&           res,
                        const tpcc_configs&                               cfg );

#define q12_order_types \
    q12_order_kv_types, tpcc_configs, q12_order_mapper, q12_order_reducer

class q12_order_line_probe {
   public:
    q12_order_line_probe( const std::unordered_map<q12_order_kv_types>& probe,
                          const tpcc_configs&                           cfg );

    const std::unordered_map<q12_order_kv_types>&    probe_;
    const tpcc_configs&                              cfg_;
};

#define q12_order_line_kv_types                                  \
    uint64_t /* Key order id */, uint64_t /* Val ol_delivery_d */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q12_order_line_kv_vec_types                         \
    uint64_t /* Key order */, std::vector<uint64_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q12_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q12_order_line_kv_types>& res,
                            const q12_order_line_probe&                  p );
void q12_order_line_reducer(
    const std::unordered_map<q12_order_line_kv_vec_types>& input,
    std::unordered_map<q12_order_line_kv_types>&           res,
    const q12_order_line_probe&                            p );

#define q12_order_line_types                                              \
    q12_order_line_kv_types, q12_order_line_probe, q12_order_line_mapper, \
        q12_order_line_reducer

#define q8_nation_kv_types                                  \
    int32_t /* Key nation id */, int32_t /* Val ignored */, \
        std::hash<int32_t>, std::equal_to<int32_t>
#define q8_nation_kv_vec_types                                \
    int32_t /* Key nation */, std::vector<int32_t> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q8_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q8_nation_kv_types>& res,
                       const EmptyProbe&                       p );
void q8_nation_reducer( const std::unordered_map<q8_nation_kv_vec_types>& input,
                        std::unordered_map<q8_nation_kv_types>&           res,
                        const EmptyProbe& p );

#define q8_nation_types \
    q8_nation_kv_types, EmptyProbe, q8_nation_mapper, q8_nation_reducer

class q8_supplier_probe {
   public:
    q8_supplier_probe( const std::unordered_map<q8_nation_kv_types>& probe,
                       const tpcc_configs&                           cfg );

    const std::unordered_map<q8_nation_kv_types>&    probe_;
    const tpcc_configs&                              cfg_;
};

#define q8_supplier_kv_types                                  \
    uint64_t /* Key supplier id */, int32_t /* Val nation */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q8_supplier_kv_vec_types                                 \
    uint64_t /* Key supplier */, std::vector<int32_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q8_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q8_supplier_kv_types>& res,
                         const q8_supplier_probe&                  p );
void q8_supplier_reducer(
    const std::unordered_map<q8_supplier_kv_vec_types>& input,
    std::unordered_map<q8_supplier_kv_types>& res, const q8_supplier_probe& p );

#define q8_supplier_types                                        \
    q8_supplier_kv_types, q8_supplier_probe, q8_supplier_mapper, \
        q8_supplier_reducer

class q8_stock_probe {
   public:
    q8_stock_probe( const std::unordered_map<q8_supplier_kv_types>& probe,
                    const tpcc_configs&                             cfg );

    const std::unordered_map<q8_supplier_kv_types>&  probe_;
    const tpcc_configs&                              cfg_;
};

#define q8_stock_kv_types                                  \
    uint64_t /* Key stock id */, int32_t /* Val supplier */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q8_stock_kv_vec_types                                 \
    uint64_t /* Key stock */, std::vector<int32_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q8_stock_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q8_stock_kv_types>& res,
                         const q8_stock_probe&                  p );
void q8_stock_reducer(
    const std::unordered_map<q8_stock_kv_vec_types>& input,
    std::unordered_map<q8_stock_kv_types>& res, const q8_stock_probe& p );

#define q8_stock_types                                        \
    q8_stock_kv_types, q8_stock_probe, q8_stock_mapper, \
        q8_stock_reducer

#define q8_order_kv_types                                              \
    uint64_t /* Key order id */, uint64_t /* Val o_entry_d as year */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q8_order_kv_vec_types                                \
    uint64_t /* Key order */, std::vector<uint64_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q8_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q8_order_kv_types>& res,
                       const EmptyProbe&                       p );
void q8_order_reducer( const std::unordered_map<q8_order_kv_vec_types>& input,
                        std::unordered_map<q8_order_kv_types>&           res,
                        const EmptyProbe& p );

#define q8_order_types \
    q8_order_kv_types, EmptyProbe, q8_order_mapper, q8_order_reducer

class q8_order_line_probe {
   public:
    q8_order_line_probe( const std::unordered_map<q8_stock_kv_types>& s_probe,
                         const std::unordered_map<q8_order_kv_types>& o_probe,
                         const tpcc_configs&                          cfg );

    const std::unordered_map<q8_stock_kv_types>&    s_probe_;
    const std::unordered_map<q8_order_kv_types>&    o_probe_;
    const tpcc_configs&                             cfg_;
};

class q8_order_line_key {
   public:
    q8_order_line_key( uint64_t stock_key, uint64_t year );

    uint64_t stock_key_;
    uint64_t year_;
};

struct q8_order_line_key_equal_functor {
    bool operator()( const q8_order_line_key& a,
                     const q8_order_line_key& b ) const {
        return ( ( a.stock_key_ == b.stock_key_ ) && ( a.year_ == b.year_ ) );
    }
};

struct q8_order_line_key_hasher {
    std::size_t operator()( const q8_order_line_key& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.stock_key_ );
        seed = folly::hash::hash_combine( seed, a.year_ );
        return seed;
    }
};

#define q8_order_line_kv_types                                              \
    q8_order_line_key, float /* Val ol_amount */, q8_order_line_key_hasher, \
        q8_order_line_key_equal_functor
#define q8_order_line_kv_vec_types                                             \
    q8_order_line_key, std::vector<float> /* Val */, q8_order_line_key_hasher, \
        q8_order_line_key_equal_functor

void q8_order_line_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q8_order_line_kv_types>& res,
                         const q8_order_line_probe&                  p );
void q8_order_line_reducer(
    const std::unordered_map<q8_order_line_kv_vec_types>& input,
    std::unordered_map<q8_order_line_kv_types>& res, const q8_order_line_probe& p );

#define q8_order_line_types                                        \
    q8_order_line_kv_types, q8_order_line_probe, q8_order_line_mapper, \
        q8_order_line_reducer

#define q9_nation_kv_types                                                \
    int32_t /* n_id */, nation /* val */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q9_nation_kv_vec_types                                            \
    int32_t /* n_id */, std::vector<nation> /* val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q9_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q9_nation_kv_types>& res,
                       const EmptyProbe&                       p );
void q9_nation_reducer( const std::unordered_map<q9_nation_kv_vec_types>& input,
                        std::unordered_map<q9_nation_kv_types>&           res,
                        const EmptyProbe&                                 p );

#define q9_nation_types \
    q9_nation_kv_types, EmptyProbe, q9_nation_mapper, q9_nation_reducer

#define q9_item_kv_types                                              \
    uint64_t /* i_id */, int32_t /* val void */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q9_item_kv_vec_types                                                  \
    uint64_t /* i_id */, std::vector<int32_t> /* val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

void q9_item_mapper( const std::vector<result_tuple>&      res_tuples,
                     std::unordered_map<q9_item_kv_types>& res,
                     const EmptyProbe&                     p );
void q9_item_reducer( const std::unordered_map<q9_item_kv_vec_types>& input,
                      std::unordered_map<q9_item_kv_types>&           res,
                      const EmptyProbe&                               p );

#define q9_item_types \
    q9_item_kv_types, EmptyProbe, q9_item_mapper, q9_item_reducer

class q9_supplier_probe {
   public:
    q9_supplier_probe( const std::unordered_map<q9_nation_kv_types>& probe,
                       const tpcc_configs&                             cfg );

    const std::unordered_map<q9_nation_kv_types>& probe_;
    const tpcc_configs&                           cfg_;
};

#define q9_supplier_kv_types                                        \
    int32_t /* s_id */, int32_t /* val nation_id */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q9_supplier_kv_vec_types                                  \
    int32_t /* s_id */, std::vector<int32_t> /* val nation_id */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q9_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q9_supplier_kv_types>& res,
                         const q9_supplier_probe&                  p );
void q9_supplier_reducer(
    const std::unordered_map<q9_supplier_kv_vec_types>& input,
    std::unordered_map<q9_supplier_kv_types>& res, const q9_supplier_probe& p );

#define q9_supplier_types                                        \
    q9_supplier_kv_types, q9_supplier_probe, q9_supplier_mapper, \
        q9_supplier_reducer

class q9_stock_probe {
   public:
    q9_stock_probe( const std::unordered_map<q9_item_kv_types>&     i_probe,
                    const std::unordered_map<q9_supplier_kv_types>& s_probe,
                    const tpcc_configs&                             cfg );

    const std::unordered_map<q9_item_kv_types>&     i_probe_;
    const std::unordered_map<q9_supplier_kv_types>& s_probe_;
    const tpcc_configs&                           cfg_;
};

#define q9_stock_kv_types                                                     \
    uint64_t /* stock_id */, int32_t /* val nation_id */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q9_stock_kv_vec_types                                          \
    uint64_t /* stock_id */, std::vector<int32_t> /* val nation_id */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q9_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q9_stock_kv_types>& res,
                      const q9_stock_probe&                  p );
void q9_stock_reducer(
    const std::unordered_map<q9_stock_kv_vec_types>& input,
    std::unordered_map<q9_stock_kv_types>& res, const q9_stock_probe& p );

#define q9_stock_types \
    q9_stock_kv_types, q9_stock_probe, q9_stock_mapper, q9_stock_reducer

#define q9_order_kv_types q8_order_kv_types
#define q9_order_kv_vec_types                                q8_order_kv_vec_types

void q9_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q9_order_kv_types>& res,
                      const EmptyProbe&                      p );
void q9_order_reducer( const std::unordered_map<q9_order_kv_vec_types>& input,
                        std::unordered_map<q9_order_kv_types>&           res,
                        const EmptyProbe& p );

#define q9_order_types \
    q9_order_kv_types, EmptyProbe, q9_order_mapper, q9_order_reducer

class q9_order_line_probe {
   public:
    q9_order_line_probe( const std::unordered_map<q9_stock_kv_types>& s_probe,
                         const std::unordered_map<q9_order_kv_types>& o_probe,
                         const tpcc_configs&                          cfg );

    const std::unordered_map<q9_stock_kv_types>&    s_probe_;
    const std::unordered_map<q9_order_kv_types>&    o_probe_;
    const tpcc_configs&                             cfg_;
};

#define q9_order_line_kv_types                                              \
    q8_order_line_key, float /* Val ol_amount */, q8_order_line_key_hasher, \
        q8_order_line_key_equal_functor
#define q9_order_line_kv_vec_types                                             \
    q8_order_line_key, std::vector<float> /* Val */, q8_order_line_key_hasher, \
        q8_order_line_key_equal_functor

void q9_order_line_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q9_order_line_kv_types>& res,
                         const q9_order_line_probe&                  p );
void q9_order_line_reducer(
    const std::unordered_map<q9_order_line_kv_vec_types>& input,
    std::unordered_map<q9_order_line_kv_types>& res, const q9_order_line_probe& p );

#define q9_order_line_types                                            \
    q9_order_line_kv_types, q9_order_line_probe, q9_order_line_mapper, \
        q9_order_line_reducer

#define q11_supplier_kv_types                                \
    int32_t /* supplier key */, int32_t, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q11_supplier_kv_vec_types                                           \
    int32_t /* Key */, std::vector<int32_t> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q11_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q11_supplier_kv_types>& res,
                          const EmptyProbe&                          p );
void q11_supplier_reducer(
    const std::unordered_map<q11_supplier_kv_vec_types>& input,
    std::unordered_map<q11_supplier_kv_types>& res, const EmptyProbe& p );

#define q11_supplier_types \
    q11_supplier_kv_types, EmptyProbe, q11_supplier_mapper, q11_supplier_reducer

class q11_stock_probe {
   public:
    q11_stock_probe( const std::unordered_map<q11_supplier_kv_types>& probe,
                     const tpcc_configs&                              cfg );

    const std::unordered_map<q11_supplier_kv_types>& probe_;
    const tpcc_configs&                              cfg_;
};

#define q11_stock_kv_types                                \
    int32_t /* item key */, int64_t, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q11_stock_kv_vec_types                                             \
    int32_t /* Key */, std::vector<int64_t> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q11_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q11_stock_kv_types>& res,
                       const q11_stock_probe&                  p );
void q11_stock_reducer( const std::unordered_map<q11_stock_kv_vec_types>& input,
                        std::unordered_map<q11_stock_kv_types>&           res,
                        const q11_stock_probe&                            p );

#define q11_stock_types \
    q11_stock_kv_types, q11_stock_probe, q11_stock_mapper, q11_stock_reducer

#define q13_order_kv_types                                                   \
    uint64_t /* cust key */, uint64_t /* num orders */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q13_order_kv_vec_types                                                \
    uint64_t /* Key */, std::vector<uint64_t> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

void q13_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q13_order_kv_types>& res,
                       const tpcc_configs&                     cfg );
void q13_order_reducer( const std::unordered_map<q13_order_kv_vec_types>& input,
                        std::unordered_map<q13_order_kv_types>&           res,
                        const tpcc_configs&                               cfg );

#define q13_order_types \
    q13_order_kv_types, tpcc_configs, q13_order_mapper, q13_order_reducer

class q13_customer_probe {
   public:
    q13_customer_probe( const std::unordered_map<q13_order_kv_types>& probe,
                        const tpcc_configs&                           cfg );

    const std::unordered_map<q13_order_kv_types>&    probe_;
    const tpcc_configs&                              cfg_;
};

#define q13_customer_kv_types                                              \
    int32_t /* cust key */, uint64_t /* num orders */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q13_customer_kv_vec_types                                           \
    int32_t /* Key */, std::vector<uint64_t> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q13_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q13_customer_kv_types>& res,
                          const q13_customer_probe&                  p );
void q13_customer_reducer(
    const std::unordered_map<q13_customer_kv_vec_types>& input,
    std::unordered_map<q13_customer_kv_types>&           res,
    const q13_customer_probe&                            p );

#define q13_customer_types                                          \
    q13_customer_kv_types, q13_customer_probe, q13_customer_mapper, \
        q13_customer_reducer

#define q15_order_line_kv_types                                               \
    uint64_t /* stock key */, float /* VAL ol_amount */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q15_order_line_kv_vec_types                         \
    uint64_t /* stock Key */, std::vector<float> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q15_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q15_order_line_kv_types>& res,
                            const tpcc_configs&                          cfg );
void q15_order_line_reducer(
    const std::unordered_map<q15_order_line_kv_vec_types>& input,
    std::unordered_map<q15_order_line_kv_types>& res, const tpcc_configs& cfg );

#define q15_order_line_types                                      \
    q15_order_line_kv_types, tpcc_configs, q15_order_line_mapper, \
        q15_order_line_reducer

class q15_stock_probe {
   public:
    q15_stock_probe( const std::unordered_map<q15_order_line_kv_types>& probe,
                     const tpcc_configs&                                cfg );

    const std::unordered_map<q15_order_line_kv_types>& probe_;
    const tpcc_configs&                                cfg_;
};

#define q15_stock_kv_types                                                     \
    int32_t /* supplier key */, float /* VAL ol_amount */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q15_stock_kv_vec_types                                \
    int32_t /* supplier Key */, std::vector<float> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q15_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q15_stock_kv_types>& res,
                       const q15_stock_probe&                  p );
void q15_stock_reducer( const std::unordered_map<q15_stock_kv_vec_types>& input,
                        std::unordered_map<q15_stock_kv_types>&           res,
                        const q15_stock_probe&                            p );

#define q15_stock_types \
    q15_stock_kv_types, q15_stock_probe, q15_stock_mapper, q15_stock_reducer

class q15_supplier_probe {
   public:
    q15_supplier_probe( const std::unordered_map<q15_stock_kv_types>& probe,
                        const tpcc_configs&                           cfg );

    const std::unordered_map<q15_stock_kv_types>&   probe_;
    const tpcc_configs&                                cfg_;
};

#define q15_supplier_kv_types                                           \
    int32_t /* supplier key */, supplier /* VAL */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q15_supplier_kv_vec_types                                \
    int32_t /* supplier Key */, std::vector<supplier> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q15_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q15_supplier_kv_types>& res,
                          const q15_supplier_probe&                  p );
void q15_supplier_reducer(
    const std::unordered_map<q15_supplier_kv_vec_types>& input,
    std::unordered_map<q15_supplier_kv_types>&           res,
    const q15_supplier_probe&                            p );

#define q15_supplier_types                                          \
    q15_supplier_kv_types, q15_supplier_probe, q15_supplier_mapper, \
        q15_supplier_reducer

#define q16_item_kv_types                                           \
    int32_t /* item key */, item /* VAL */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q16_item_kv_vec_types                                                \
    int32_t /* item Key */, std::vector<item> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q16_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q16_item_kv_types>& res,
                      const EmptyProbe&                      p );
void q16_item_reducer( const std::unordered_map<q16_item_kv_vec_types>& input,
                       std::unordered_map<q16_item_kv_types>&           res,
                       const EmptyProbe&                                p );

#define q16_item_types \
    q16_item_kv_types, EmptyProbe, q16_item_mapper, q16_item_reducer

#define q16_supplier_kv_types                                          \
    int32_t /* supplier key */, int32_t /* VAL ignore */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q16_supplier_kv_vec_types                               \
    int32_t /* supplier Key */, std::vector<int32_t> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q16_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q16_supplier_kv_types>& res,
                          const EmptyProbe&                          p );
void q16_supplier_reducer(
    const std::unordered_map<q16_supplier_kv_vec_types>& input,
    std::unordered_map<q16_supplier_kv_types>& res, const EmptyProbe& p );

#define q16_supplier_types \
    q16_supplier_kv_types, EmptyProbe, q16_supplier_mapper, q16_supplier_reducer

#define q16_stock_kv_types                                                 \
    int32_t /* item key */, uint64_t /* VAL count */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q16_stock_kv_vec_types                               \
    int32_t /* item Key */, std::vector<uint64_t> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

class q16_stock_probe {
   public:
    q16_stock_probe( const std::unordered_map<q16_item_kv_types>&     i_probe,
                     const std::unordered_map<q16_supplier_kv_types>& s_probe,
                     const tpcc_configs&                              cfg );

    const std::unordered_map<q16_item_kv_types>&     i_probe_;
    const std::unordered_map<q16_supplier_kv_types>& s_probe_;
    const tpcc_configs&                              cfg_;
};

void q16_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q16_stock_kv_types>& res,
                       const q16_stock_probe&                  p );
void q16_stock_reducer( const std::unordered_map<q16_stock_kv_vec_types>& input,
                        std::unordered_map<q16_stock_kv_types>&           res,
                        const q16_stock_probe&                            p );

#define q16_stock_types \
    q16_stock_kv_types, q16_stock_probe, q16_stock_mapper, q16_stock_reducer

#define q19_item_kv_types                                           \
    int32_t /* item key */, int32_t /* VAL */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q19_item_kv_vec_types                               \
    int32_t /* item Key */, std::vector<int32_t> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q19_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q19_item_kv_types>& res,
                      const EmptyProbe&                      p );
void q19_item_reducer( const std::unordered_map<q19_item_kv_vec_types>& input,
                       std::unordered_map<q19_item_kv_types>&           res,
                       const EmptyProbe&                                p );

#define q19_item_types \
    q19_item_kv_types, EmptyProbe, q19_item_mapper, q19_item_reducer

#define q19_order_line_kv_types                                            \
    int32_t /* item key */, float /* VAL ol_amount */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q19_order_line_kv_vec_types                                           \
    int32_t /* item Key */, std::vector<float> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

class q19_order_line_probe {
   public:
    q19_order_line_probe( const std::unordered_map<q19_item_kv_types>& probe,
                          const tpcc_configs&                          cfg );

    const std::unordered_map<q19_item_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

void q19_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q19_order_line_kv_types>& res,
                            const q19_order_line_probe&                  p );
void q19_order_line_reducer(
    const std::unordered_map<q19_order_line_kv_vec_types>& input,
    std::unordered_map<q19_order_line_kv_types>&           res,
    const q19_order_line_probe&                            p );

#define q19_order_line_types                                              \
    q19_order_line_kv_types, q19_order_line_probe, q19_order_line_mapper, \
        q19_order_line_reducer

#define q22_order_kv_types                                  \
    uint64_t /* customer key */, int32_t /* VAL ignored */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q22_order_kv_vec_types                               \
    uint64_t /* item Key */, std::vector<int32_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q22_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q22_order_kv_types>& res,
                       const tpcc_configs&                     cfg );
void q22_order_reducer( const std::unordered_map<q22_order_kv_vec_types>& input,
                        std::unordered_map<q22_order_kv_types>&           res,
                        const tpcc_configs&                               cfg );

#define q22_order_types \
    q22_order_kv_types, tpcc_configs, q22_order_mapper, q22_order_reducer

#define q22_customer_kv_types                                             \
    uint64_t /* customer key */, customer /* VAL */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q22_customer_kv_vec_types                                 \
    uint64_t /* customer Key */, std::vector<customer> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q22_customer_probe {
   public:
    q22_customer_probe( const std::unordered_map<q22_order_kv_types>& probe,
                        const tpcc_configs&                           cfg );

    const std::unordered_map<q22_order_kv_types>& probe_;
    const tpcc_configs&                           cfg_;
};

void q22_customer_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q22_customer_kv_types>& res,
                          const q22_customer_probe&                  p );
void q22_customer_reducer(
    const std::unordered_map<q22_customer_kv_vec_types>& input,
    std::unordered_map<q22_customer_kv_types>&           res,
    const q22_customer_probe&                            p );

#define q22_customer_types                                          \
    q22_customer_kv_types, q22_customer_probe, q22_customer_mapper, \
        q22_customer_reducer

#define q20_item_kv_types                                       \
    int32_t /* item key */, item /* VAL */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q20_item_kv_vec_types                                                \
    int32_t /* item Key */, std::vector<item> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q20_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q20_item_kv_types>& res,
                      const EmptyProbe&                      p );
void q20_item_reducer( const std::unordered_map<q20_item_kv_vec_types>& input,
                       std::unordered_map<q20_item_kv_types>&           res,
                       const EmptyProbe&                                p );

#define q20_item_types \
    q20_item_kv_types, EmptyProbe, q20_item_mapper, q20_item_reducer

#define q20_stock_kv_types                                          \
    uint64_t /* stock key */, stock /* VAL */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q20_stock_kv_vec_types                              \
    uint64_t /* stock Key */, std::vector<stock> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q20_stock_probe {
   public:
    q20_stock_probe( const std::unordered_map<q20_item_kv_types>& probe,
                     const tpcc_configs&                          cfg );

    const std::unordered_map<q20_item_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

void q20_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q20_stock_kv_types>& res,
                       const q20_stock_probe&                  p );
void q20_stock_reducer( const std::unordered_map<q20_stock_kv_vec_types>& input,
                        std::unordered_map<q20_stock_kv_types>&           res,
                        const q20_stock_probe&                            p );

#define q20_stock_types \
    q20_stock_kv_types, q20_stock_probe, q20_stock_mapper, q20_stock_reducer

#define q20_order_line_kv_types                            \
    uint64_t /* stock key */, float /* VAL ol_quantity */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q20_order_line_kv_vec_types                              \
    uint64_t /* stock Key */, std::vector<float> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q20_order_line_probe {
   public:
    q20_order_line_probe( const std::unordered_map<q20_stock_kv_types>& probe,
                          const tpcc_configs&                           cfg );

    const std::unordered_map<q20_stock_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

void q20_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q20_order_line_kv_types>& res,
                            const q20_order_line_probe&                  p );
void q20_order_line_reducer(
    const std::unordered_map<q20_order_line_kv_vec_types>& input,
    std::unordered_map<q20_order_line_kv_types>&           res,
    const q20_order_line_probe&                            p );

#define q20_order_line_types                                              \
    q20_order_line_kv_types, q20_order_line_probe, q20_order_line_mapper, \
        q20_order_line_reducer

#define q20_supplier_kv_types                                           \
    int32_t /* supplier key */, supplier /* VAL */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q20_supplier_kv_vec_types                                \
    int32_t /* supplier Key */, std::vector<supplier> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

class q20_supplier_probe {
   public:
    q20_supplier_probe(
        const std::unordered_map<q20_stock_kv_types>&      s_probe,
        const std::unordered_map<q20_order_line_kv_types>& ol_probe,
        const tpcc_configs&                                cfg );
    void compute( float s_quant_multi );

    const std::unordered_map<q20_stock_kv_types>&      s_probe_;
    const std::unordered_map<q20_order_line_kv_types>& ol_probe_;
    const tpcc_configs&                                cfg_;

    std::unordered_set<int32_t> supplier_ids_;
};

void q20_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q20_supplier_kv_types>& res,
                          const q20_supplier_probe&                  p );
void q20_supplier_reducer(
    const std::unordered_map<q20_supplier_kv_vec_types>& input,
    std::unordered_map<q20_supplier_kv_types>&           res,
    const q20_supplier_probe&                            p );

#define q20_supplier_types                                          \
    q20_supplier_kv_types, q20_supplier_probe, q20_supplier_mapper, \
        q20_supplier_reducer

#define q21_supplier_kv_types                                           \
    int32_t /* supplier key */, supplier /* VAL */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q21_supplier_kv_vec_types                                \
    int32_t /* supplier Key */, std::vector<supplier> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q21_supplier_mapper( const std::vector<result_tuple>&           res_tuples,
                          std::unordered_map<q21_supplier_kv_types>& res,
                          const EmptyProbe&                  p );
void q21_supplier_reducer(
    const std::unordered_map<q21_supplier_kv_vec_types>& input,
    std::unordered_map<q21_supplier_kv_types>& res, const EmptyProbe& p );

#define q21_supplier_types \
    q21_supplier_kv_types, EmptyProbe, q21_supplier_mapper, q21_supplier_reducer

#define q21_stock_kv_types                                   \
    uint64_t /* stock key */, int32_t /* VAL supplier_id */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q21_stock_kv_vec_types                                \
    uint64_t /* stock Key */, std::vector<int32_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q21_stock_probe {
   public:
    q21_stock_probe( const std::unordered_map<q21_supplier_kv_types>& probe,
                     const tpcc_configs&                              cfg );

    const std::unordered_map<q21_supplier_kv_types>& probe_;
    const tpcc_configs&                              cfg_;
};

void q21_stock_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q21_stock_kv_types>& res,
                       const q21_stock_probe&                  p );
void q21_stock_reducer( const std::unordered_map<q21_stock_kv_vec_types>& input,
                        std::unordered_map<q21_stock_kv_types>&           res,
                        const q21_stock_probe&                            p );

#define q21_stock_types \
    q21_stock_kv_types, q21_stock_probe, q21_stock_mapper, q21_stock_reducer

#define q21_order_kv_types                                        \
    uint64_t /* order key */, uint64_t /* VAL o_entry_d_since */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q21_order_kv_vec_types                                 \
    uint64_t /* order Key */, std::vector<uint64_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q21_order_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q21_order_kv_types>& res,
                       const EmptyProbe&                       p );
void q21_order_reducer( const std::unordered_map<q21_order_kv_vec_types>& input,
                        std::unordered_map<q21_order_kv_types>&           res,
                        const EmptyProbe&                                 p );

#define q21_order_types \
    q21_order_kv_types, EmptyProbe, q21_order_mapper, q21_order_reducer

#define q21_order_line_kv_types                                                \
    uint64_t /* ol key */, uint64_t /* VAL  stock_key */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q21_order_line_kv_vec_types                            \
    uint64_t /* ol Key */, std::vector<uint64_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q21_order_line_probe {
   public:
    q21_order_line_probe( const std::unordered_map<q21_stock_kv_types>& s_probe,
                          const std::unordered_map<q21_order_kv_types>& o_probe,
                          const tpcc_configs&                           cfg );

    const std::unordered_map<q21_stock_kv_types>& s_probe_;
    const std::unordered_map<q21_order_kv_types>& o_probe_;
    const tpcc_configs&                           cfg_;
};

void q21_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q21_order_line_kv_types>& res,
                            const q21_order_line_probe&                  p );
void q21_order_line_reducer(
    const std::unordered_map<q21_order_line_kv_vec_types>& input,
    std::unordered_map<q21_order_line_kv_types>&           res,
    const q21_order_line_probe&                            p );

#define q21_order_line_types                                              \
    q21_order_line_kv_types, q21_order_line_probe, q21_order_line_mapper, \
        q21_order_line_reducer

#define q17_item_kv_types                                       \
    int32_t /* item key */, int32_t /* VAL  match predicate */, \
        std::hash<int32_t>, std::equal_to<int32_t>
#define q17_item_kv_vec_types                               \
    int32_t /* item Key */, std::vector<int32_t> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q17_item_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q17_item_kv_types>& res,
                      const EmptyProbe&                      p );
void q17_item_reducer( const std::unordered_map<q17_item_kv_vec_types>& input,
                       std::unordered_map<q17_item_kv_types>&           res,
                       const EmptyProbe&                                p );

#define q17_item_types \
    q17_item_kv_types, EmptyProbe, q17_item_mapper, q17_item_reducer

class q17_order_line_probe {
   public:
    q17_order_line_probe( const std::unordered_map<q17_item_kv_types>& probe,
                          const tpcc_configs&                          cfg );

    const std::unordered_map<q17_item_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

#define q17_order_line_kv_types                                    \
    int32_t /* item key */, q1_agg /* VAL  */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q17_order_line_kv_vec_types                                            \
    int32_t /* item Key */, std::vector<q1_agg> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q17_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<q17_order_line_kv_types>& res,
                            const q17_order_line_probe&                  p );
void q17_order_line_reducer(
    const std::unordered_map<q17_order_line_kv_vec_types>& input,
    std::unordered_map<q17_order_line_kv_types>&           res,
    const q17_order_line_probe&                            p );

#define q17_order_line_types                                              \
    q17_order_line_kv_types, q17_order_line_probe, q17_order_line_mapper, \
        q17_order_line_reducer

#define q2_nation_kv_types                                           \
    int32_t /* nation key */, nation /* VAL  */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q2_nation_kv_vec_types                               \
    int32_t /* nation Key */, std::vector<nation> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q2_nation_mapper( const std::vector<result_tuple>&        res_tuples,
                       std::unordered_map<q2_nation_kv_types>& res,
                       const EmptyProbe&                       p );
void q2_nation_reducer( const std::unordered_map<q2_nation_kv_vec_types>& input,
                        std::unordered_map<q2_nation_kv_types>&           res,
                        const EmptyProbe&                                 p );

#define q2_nation_types \
    q2_nation_kv_types, EmptyProbe, q2_nation_mapper, q2_nation_reducer

#define q2_supplier_kv_types                                             \
    int32_t /* supplier key */, supplier /* VAL  */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q2_supplier_kv_vec_types                                 \
    int32_t /* supplier Key */, std::vector<supplier> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

class q2_supplier_probe {
   public:
    q2_supplier_probe( const std::unordered_map<q2_nation_kv_types>& probe,
                       const tpcc_configs&                           cfg );

    const std::unordered_map<q2_nation_kv_types>& probe_;
    const tpcc_configs&                          cfg_;
};

void q2_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q2_supplier_kv_types>& res,
                         const q2_supplier_probe&                  p );
void q2_supplier_reducer(
    const std::unordered_map<q2_supplier_kv_vec_types>& input,
    std::unordered_map<q2_supplier_kv_types>& res, const q2_supplier_probe& p );

#define q2_supplier_types                                        \
    q2_supplier_kv_types, q2_supplier_probe, q2_supplier_mapper, \
        q2_supplier_reducer

#define q2_item_kv_types                                        \
    int32_t /*item key */, item /* VAL  */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q2_item_kv_vec_types                                                 \
    int32_t /* item Key */, std::vector<item> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

void q2_item_mapper( const std::vector<result_tuple>&      res_tuples,
                     std::unordered_map<q2_item_kv_types>& res,
                     const EmptyProbe&                     p );
void q2_item_reducer( const std::unordered_map<q2_item_kv_vec_types>& input,
                      std::unordered_map<q2_item_kv_types>&           res,
                      const EmptyProbe&                               p );

#define q2_item_types \
    q2_item_kv_types, EmptyProbe, q2_item_mapper, q2_item_reducer

#define q2_stock_kv_types                                                  \
    int32_t /* item key */, stock /* s_quantity  */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q2_stock_kv_vec_types                                                 \
    int32_t /* item Key */, std::vector<stock> /* Val */, std::hash<int32_t>, \
        std::equal_to<int32_t>

class q2_stock_probe {
   public:
    q2_stock_probe( const std::unordered_map<q2_supplier_kv_types>& s_probe,
                    const std::unordered_map<q2_item_kv_types>&  i_probe,
                    const tpcc_configs&                          cfg );

    const std::unordered_map<q2_supplier_kv_types>& s_probe_;
    const std::unordered_map<q2_item_kv_types>&     i_probe_;
    const tpcc_configs&                             cfg_;
};

void q2_stock_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q2_stock_kv_types>& res,
                         const q2_stock_probe&                  p );
void q2_stock_reducer(
    const std::unordered_map<q2_stock_kv_vec_types>& input,
    std::unordered_map<q2_stock_kv_types>& res, const q2_stock_probe& p );

#define q2_stock_types \
    q2_stock_kv_types, q2_stock_probe, q2_stock_mapper, q2_stock_reducer

#define q7_supplier_kv_types                                             \
    int32_t /* supplier key */, int32_t /* n_id  */, std::hash<int32_t>, \
        std::equal_to<int32_t>
#define q7_supplier_kv_vec_types                                \
    int32_t /* supplier Key */, std::vector<int32_t> /* Val */, \
        std::hash<int32_t>, std::equal_to<int32_t>

void q7_supplier_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q7_supplier_kv_types>& res,
                         const EmptyProbe&                         p );
void q7_supplier_reducer(
    const std::unordered_map<q7_supplier_kv_vec_types>& input,
    std::unordered_map<q7_supplier_kv_types>& res, const EmptyProbe& p );

#define q7_supplier_types \
    q7_supplier_kv_types, EmptyProbe, q7_supplier_mapper, q7_supplier_reducer

#define q7_stock_kv_types                                                  \
    uint64_t /* stock key */, int32_t /* VAL s_id */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q7_stock_kv_vec_types                                 \
    uint64_t /* stock Key */, std::vector<int32_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q7_stock_probe {
   public:
    q7_stock_probe( const std::unordered_map<q7_supplier_kv_types>& probe,
                    const tpcc_configs&                             cfg );

    const std::unordered_map<q7_supplier_kv_types>& probe_;
    const tpcc_configs&                             cfg_;
};

void q7_stock_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q7_stock_kv_types>& res,
                      const q7_stock_probe&                  p );
void q7_stock_reducer( const std::unordered_map<q7_stock_kv_vec_types>& input,
                       std::unordered_map<q7_stock_kv_types>&           res,
                       const q7_stock_probe&                            p );

#define q7_stock_types \
    q7_stock_kv_types, q7_stock_probe, q7_stock_mapper, q7_stock_reducer

#define q7_customer_kv_types                                                  \
    uint64_t /* customer key */, int32_t /* VAL n_id */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define q7_customer_kv_vec_types                                 \
    uint64_t /* customer Key */, std::vector<int32_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

void q7_customer_mapper( const std::vector<result_tuple>&          res_tuples,
                         std::unordered_map<q7_customer_kv_types>& res,
                         const EmptyProbe&                         p );
void q7_customer_reducer(
    const std::unordered_map<q7_customer_kv_vec_types>& input,
    std::unordered_map<q7_customer_kv_types>& res, const EmptyProbe& p );

#define q7_customer_types \
    q7_customer_kv_types, EmptyProbe, q7_customer_mapper, q7_customer_reducer

#define q7_order_kv_types                                     \
    uint64_t /* order key */, uint64_t /* VAL customer id */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>
#define q7_order_kv_vec_types                                  \
    uint64_t /* order Key */, std::vector<uint64_t> /* Val */, \
        std::hash<uint64_t>, std::equal_to<uint64_t>

class q7_order_probe {
   public:
    q7_order_probe( const std::unordered_map<q7_customer_kv_types>& probe,
                    const tpcc_configs&                             cfg );

    const std::unordered_map<q7_customer_kv_types>& probe_;
    const tpcc_configs&                             cfg_;
};

void q7_order_mapper( const std::vector<result_tuple>&       res_tuples,
                      std::unordered_map<q7_order_kv_types>& res,
                      const q7_order_probe&                  p );
void q7_order_reducer( const std::unordered_map<q7_order_kv_vec_types>& input,
                       std::unordered_map<q7_order_kv_types>&           res,
                       const q7_order_probe&                            p );

#define q7_order_types \
    q7_order_kv_types, q7_order_probe, q7_order_mapper, q7_order_reducer

class q7_order_line_key {
   public:
    q7_order_line_key( uint64_t stock_key, uint64_t customer_key,
                       uint64_t year );

    uint64_t stock_key_;
    uint64_t customer_key_;
    uint64_t year_;
};

struct q7_order_line_key_equal_functor {
    bool operator()( const q7_order_line_key& a,
                     const q7_order_line_key& b ) const {
        return ( ( a.stock_key_ == b.stock_key_ ) &&
                 ( a.customer_key_ == b.customer_key_ ) &&
                 ( a.year_ == b.year_ ) );
    }
};

struct q7_order_line_key_hasher {
    std::size_t operator()( const q7_order_line_key& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.stock_key_ );
        seed = folly::hash::hash_combine( seed, a.customer_key_ );
        seed = folly::hash::hash_combine( seed, a.year_ );
        return seed;
    }
};

#define q7_order_line_kv_types                                 \
    q7_order_line_key /* ol key */, float /* VAL ol_amount */, \
        q7_order_line_key_hasher, q7_order_line_key_equal_functor
#define q7_order_line_kv_vec_types                                   \
    q7_order_line_key /* order Key */, std::vector<float> /* Val */, \
        q7_order_line_key_hasher, q7_order_line_key_equal_functor

class q7_order_line_probe {
   public:
    q7_order_line_probe( const std::unordered_map<q7_order_kv_types>& o_probe,
                         const std::unordered_map<q7_stock_kv_types>& s_probe,
                         const tpcc_configs&                          cfg );

    const std::unordered_map<q7_order_kv_types>&    o_probe_;
    const std::unordered_map<q7_stock_kv_types>&    s_probe_;
    const tpcc_configs&                             cfg_;
};

void q7_order_line_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<q7_order_line_kv_types>& res,
                           const q7_order_line_probe&                  p );
void q7_order_line_reducer(
    const std::unordered_map<q7_order_line_kv_vec_types>& input,
    std::unordered_map<q7_order_line_kv_types>&           res,
    const q7_order_line_probe&                            p );

#define q7_order_line_types                                            \
    q7_order_line_kv_types, q7_order_line_probe, q7_order_line_mapper, \
        q7_order_line_reducer

class q7_res_key {
   public:
    q7_res_key( int32_t supplier_n_id, int32_t customer_n_id, uint64_t year );

    int32_t supplier_n_id_;
    int32_t  customer_n_id_;
    uint64_t year_;
};

struct q7_res_key_equal_functor {
    bool operator()( const q7_res_key& a, const q7_res_key& b ) const {
        return ( ( a.supplier_n_id_ == b.supplier_n_id_ ) &&
                 ( a.customer_n_id_ == b.customer_n_id_ ) &&
                 ( a.year_ == b.year_ ) );
    }
};

struct q7_res_key_hasher {
    std::size_t operator()( const q7_res_key& a ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, a.supplier_n_id_ );
        seed = folly::hash::hash_combine( seed, a.customer_n_id_ );
        seed = folly::hash::hash_combine( seed, a.year_ );
        return seed;
    }
};

