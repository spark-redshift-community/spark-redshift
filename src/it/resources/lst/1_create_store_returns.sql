/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

create table "PUBLIC"."store_returns"
(
    sr_returned_date_sk int4 ,
    sr_return_time_sk int4 ,
    sr_item_sk int4 not null ,
    sr_customer_sk int4 ,
    sr_cdemo_sk int4 ,
    sr_hdemo_sk int4 ,
    sr_addr_sk int4 ,
    sr_store_sk int4 ,
    sr_reason_sk int4 ,
    sr_ticket_number int8 not null,
    sr_return_quantity int4 ,
    sr_return_amt numeric(7,2) ,
    sr_return_tax numeric(7,2) ,
    sr_return_amt_inc_tax numeric(7,2) ,
    sr_fee numeric(7,2) ,
    sr_return_ship_cost numeric(7,2) ,
    sr_refunded_cash numeric(7,2) ,
    sr_reversed_charge numeric(7,2) ,
    sr_store_credit numeric(7,2) ,
    sr_net_loss numeric(7,2)
);