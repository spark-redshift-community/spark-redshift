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

INSERT INTO "PUBLIC"."date_dim" (
    d_date_sk,
    d_date_id,
    d_date,
    d_month_seq,
    d_week_seq,
    d_quarter_seq,
    d_year,
    d_dow,
    d_moy,
    d_dom,
    d_qoy,
    d_fy_year,
    d_fy_quarter_seq,
    d_fy_week_seq,
    d_day_name,
    d_quarter_name,
    d_holiday,
    d_weekend,
    d_following_holiday,
    d_first_dom,
    d_last_dom,
    d_same_day_ly,
    d_same_day_lq,
    d_current_day,
    d_current_week,
    d_current_month,
    d_current_quarter,
    d_current_year
) VALUES
(1, '20240101', '2024-01-01', 1, 1, 1, 2024, 1, 1, 1, 1, 2024, 1, 1, 'Monday', 'Q1', 'N', 'N', 'Y', 1, 31, 1, 52, 'Y', 'Y', 'Y', 'Y', 'Y'),
(2, '20240102', '2024-01-02', 1, 1, 1, 2024, 2, 1, 2, 1, 2024, 1, 1, 'Tuesday', 'Q1', 'N', 'N', 'N', 1, 31, 2, 52, 'N', 'Y', 'Y', 'Y', 'Y'),
(3, '20240103', '2024-01-03', 1, 1, 1, 2024, 3, 1, 3, 1, 2024, 1, 1, 'Wednesday', 'Q1', 'N', 'N', 'N', 1, 31, 3, 52, 'N', 'N', 'Y', 'Y', 'Y'),
(4, '20240104', '2024-01-04', 1, 1, 1, 2024, 4, 1, 4, 1, 2024, 1, 1, 'Thursday', 'Q1', 'N', 'N', 'N', 1, 31, 4, 52, 'N', 'N', 'N', 'Y', 'Y'),
(5, '20240105', '2024-01-05', 1, 1, 1, 2024, 5, 1, 5, 1, 2024, 1, 1, 'Friday', 'Q1', 'N', 'Y', 'N', 1, 31, 5, 52, 'N', 'N', 'N', 'N', 'Y'),
(6, '20240106', '2024-01-06', 1, 1, 1, 2024, 6, 1, 6, 1, 2024, 1, 1, 'Saturday', 'Q1', 'N', 'Y', 'Y', 1, 31, 6, 52, 'N', 'N', 'N', 'N', 'Y'),
(7, '20240107', '2024-01-07', 1, 1, 1, 2024, 7, 1, 7, 1, 2024, 1, 1, 'Sunday', 'Q1', 'N', 'Y', 'Y', 1, 31, 7, 52, 'N', 'N', 'N', 'N', 'Y'),
(8, '20240108', '2024-01-08', 2, 2, 1, 2024, 1, 1, 8, 1, 2024, 1, 2, 'Monday', 'Q1', 'N', 'N', 'Y', 1, 31, 8, 53, 'Y', 'Y', 'Y', 'Y', 'Y'),
(9, '20240109', '2024-01-09', 2, 2, 1, 2024, 2, 1, 9, 1, 2024, 1, 2, 'Tuesday', 'Q1', 'N', 'N', 'N', 1, 31, 9, 53, 'N', 'Y', 'Y', 'Y', 'Y'),
(10, '20240110', '2024-01-10', 2, 2, 1, 2024, 3, 1, 10, 1, 2024, 1, 2, 'Wednesday', 'Q1', 'N', 'N', 'N', 1, 31, 10, 53, 'N', 'N', 'Y', 'Y', 'Y'),
(11, '20240111', '2024-01-11', 2, 2, 1, 2024, 4, 1, 11, 1, 2024, 1, 2, 'Thursday', 'Q1', 'N', 'N', 'N', 1, 31, 11, 53, 'N', 'N', 'N', 'Y', 'Y'),
(12, '20240112', '2024-01-12', 2, 2, 1, 2024, 5, 1, 12, 1, 2024, 1, 2, 'Friday', 'Q1', 'N', 'Y', 'N', 1, 31, 12, 53, 'N', 'N', 'N', 'N', 'Y'),
(13, '20240113', '2024-01-13', 2, 2, 1, 2024, 6, 1, 13, 1, 2024, 1, 2, 'Saturday', 'Q1', 'N', 'Y', 'Y', 1, 31, 13, 53, 'N', 'N', 'N', 'N', 'Y'),
(14, '20240114', '2024-01-14', 2, 2, 1, 2024, 7, 1, 14, 1, 2024, 1, 2, 'Sunday', 'Q1', 'N', 'Y', 'Y', 1, 31, 14, 53, 'N', 'N', 'N', 'N', 'Y'),
(15, '20240115', '2024-01-15', 3, 3, 1, 2024, 1, 2, 15, 1, 2024, 1, 3, 'Monday', 'Q1', 'N', 'N', 'Y', 1, 29, 15, 1, 'Y', 'Y', 'Y', 'Y', 'Y'),
(16, '20240116', '2024-01-16', 3, 3, 1, 2024, 2, 2, 16, 1, 2024, 1, 3, 'Tuesday', 'Q1', 'N', 'N', 'N', 1, 29, 16, 1, 'N', 'Y', 'Y', 'Y', 'Y'),
(17, '20240117', '2024-01-17', 3, 3, 1, 2024, 3, 2, 17, 1, 2024, 1, 3, 'Wednesday', 'Q1', 'N', 'N', 'N', 1, 29, 17, 1, 'N', 'N', 'Y', 'Y', 'Y'),
(18, '20240118', '2024-01-18', 3, 3, 1, 2024, 4, 2, 18, 1, 2024, 1, 3, 'Thursday', 'Q1', 'N', 'N', 'N', 1, 29, 18, 1, 'N', 'N', 'N', 'Y', 'Y'),
(19, '20240119', '2024-01-19', 3, 3, 1, 2024, 5, 2, 19, 1, 2024, 1, 3, 'Friday', 'Q1', 'N', 'Y', 'N', 1, 29, 19, 1, 'N', 'N', 'N', 'N', 'Y'),
(20, '20240120', '2024-01-20', 3, 3, 1, 2024, 6, 2, 20, 1, 2024, 1, 3, 'Saturday', 'Q1', 'N', 'Y', 'Y', 1, 29, 20, 1, 'N', 'N', 'N', 'N', 'Y');
