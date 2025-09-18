/*
 * Copyright (c) Microsoft Corporation.
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE "PUBLIC"."catalog_sales"(
    cs_sold_time_sk           int                           ,
    cs_ship_date_sk           int                           ,
    cs_bill_customer_sk       int                           ,
    cs_bill_cdemo_sk          int                           ,
    cs_bill_hdemo_sk          int                           ,
    cs_bill_addr_sk           int                           ,
    cs_ship_customer_sk       int                           ,
    cs_ship_cdemo_sk          int                           ,
    cs_ship_hdemo_sk          int                           ,
    cs_ship_addr_sk           int                           ,
    cs_call_center_sk         int                           ,
    cs_catalog_page_sk        int                           ,
    cs_ship_mode_sk           int                           ,
    cs_warehouse_sk           int                           ,
    cs_item_sk                int                           ,
    cs_promo_sk               int                           ,
    cs_order_number           bigint                        ,
    cs_quantity               int                           ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  ,
    cs_sold_date_sk           int
) -- WITH (location='${data_path}${experiment_start_time}/${repetition}/catalog_sales/', ${partition_spec_keyword}=ARRAY['cs_sold_date_sk'] ${tblproperties_suffix});