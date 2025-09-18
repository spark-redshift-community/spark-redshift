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

create table "PUBLIC"."web_sales"
(
    ws_sold_date_sk int4 ,
    ws_sold_time_sk int4 ,
    ws_ship_date_sk int4 ,
    ws_item_sk int4 not null ,
    ws_bill_customer_sk int4 ,
    ws_bill_cdemo_sk int4 ,
    ws_bill_hdemo_sk int4 ,
    ws_bill_addr_sk int4 ,
    ws_ship_customer_sk int4 ,
    ws_ship_cdemo_sk int4 ,
    ws_ship_hdemo_sk int4 ,
    ws_ship_addr_sk int4 ,
    ws_web_page_sk int4 ,
    ws_web_site_sk int4 ,
    ws_ship_mode_sk int4 ,
    ws_warehouse_sk int4 ,
    ws_promo_sk int4 ,
    ws_order_number int8 not null,
    ws_quantity int4 ,
    ws_wholesale_cost numeric(7,2) ,
    ws_list_price numeric(7,2) ,
    ws_sales_price numeric(7,2) ,
    ws_ext_discount_amt numeric(7,2) ,
    ws_ext_sales_price numeric(7,2) ,
    ws_ext_wholesale_cost numeric(7,2) ,
    ws_ext_list_price numeric(7,2) ,
    ws_ext_tax numeric(7,2) ,
    ws_coupon_amt numeric(7,2) ,
    ws_ext_ship_cost numeric(7,2) ,
    ws_net_paid numeric(7,2) ,
    ws_net_paid_inc_tax numeric(7,2) ,
    ws_net_paid_inc_ship numeric(7,2) ,
    ws_net_paid_inc_ship_tax numeric(7,2) ,
    ws_net_profit numeric(7,2)
);