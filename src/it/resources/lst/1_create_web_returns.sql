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

create table "PUBLIC"."web_returns"
(
    wr_returned_date_sk int4 ,
    wr_returned_time_sk int4 ,
    wr_item_sk int4 not null ,
    wr_refunded_customer_sk int4 ,
    wr_refunded_cdemo_sk int4 ,
    wr_refunded_hdemo_sk int4 ,
    wr_refunded_addr_sk int4 ,
    wr_returning_customer_sk int4 ,
    wr_returning_cdemo_sk int4 ,
    wr_returning_hdemo_sk int4 ,
    wr_returning_addr_sk int4 ,
    wr_web_page_sk int4 ,
    wr_reason_sk int4 ,
    wr_order_number int8 not null,
    wr_return_quantity int4 ,
    wr_return_amt numeric(7,2) ,
    wr_return_tax numeric(7,2) ,
    wr_return_amt_inc_tax numeric(7,2) ,
    wr_fee numeric(7,2) ,
    wr_return_ship_cost numeric(7,2) ,
    wr_refunded_cash numeric(7,2) ,
    wr_reversed_charge numeric(7,2) ,
    wr_account_credit numeric(7,2) ,
    wr_net_loss numeric(7,2)
);