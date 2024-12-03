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

CREATE TABLE "PUBLIC"."date_dim"(
    d_date_sk                 int                           ,
    d_date_id                 varchar(16)                      ,
    d_date                    date                          ,
    d_month_seq               int                           ,
    d_week_seq                int                           ,
    d_quarter_seq             int                           ,
    d_year                    int                           ,
    d_dow                     int                           ,
    d_moy                     int                           ,
    d_dom                     int                           ,
    d_qoy                     int                           ,
    d_fy_year                 int                           ,
    d_fy_quarter_seq          int                           ,
    d_fy_week_seq             int                           ,
    d_day_name                varchar(9)                       ,
    d_quarter_name            varchar(6)                       ,
    d_holiday                 varchar(1)                       ,
    d_weekend                 varchar(1)                       ,
    d_following_holiday       varchar(1)                       ,
    d_first_dom               int                           ,
    d_last_dom                int                           ,
    d_same_day_ly             int                           ,
    d_same_day_lq             int                           ,
    d_current_day             varchar(1)                       ,
    d_current_week            varchar(1)                       ,
    d_current_month           varchar(1)                       ,
    d_current_quarter         varchar(1)                       ,
    d_current_year            varchar(1)
) -- WITH (location='${data_path}${experiment_start_time}/${repetition}/date_dim/' ${tblproperties_suffix});