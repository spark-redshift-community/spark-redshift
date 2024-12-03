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

INSERT INTO "PUBLIC"."web_sales" (
    ws_sold_date_sk,
    ws_sold_time_sk,
    ws_ship_date_sk,
    ws_item_sk,
    ws_bill_customer_sk,
    ws_bill_cdemo_sk,
    ws_bill_hdemo_sk,
    ws_bill_addr_sk,
    ws_ship_customer_sk,
    ws_ship_cdemo_sk,
    ws_ship_hdemo_sk,
    ws_ship_addr_sk,
    ws_web_page_sk,
    ws_web_site_sk,
    ws_ship_mode_sk,
    ws_warehouse_sk,
    ws_promo_sk,
    ws_order_number,
    ws_quantity,
    ws_wholesale_cost,
    ws_list_price,
    ws_sales_price,
    ws_ext_discount_amt,
    ws_ext_sales_price,
    ws_ext_wholesale_cost,
    ws_ext_list_price,
    ws_ext_tax,
    ws_coupon_amt,
    ws_ext_ship_cost,
    ws_net_paid,
    ws_net_paid_inc_tax,
    ws_net_paid_inc_ship,
    ws_net_paid_inc_ship_tax,
    ws_net_profit
)
VALUES
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 123456789, 1, 10.00, 15.00, 12.00, 1.00, 12.00, 10.00, 15.00, 1.20, 0.50, 2.00, 10.30, 11.50, 12.30, 13.50, 2.00),
    (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 234567890, 2, 20.00, 30.00, 24.00, 2.00, 24.00, 20.00, 30.00, 2.40, 1.00, 3.00, 20.60, 23.00, 24.60, 27.00, 4.00),
    (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 345678901, 3, 30.00, 45.00, 36.00, 3.00, 36.00, 30.00, 45.00, 3.60, 1.50, 4.00, 30.90, 34.50, 36.90, 40.50, 6.00),
    (4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 456789012, 4, 40.00, 60.00, 48.00, 4.00, 48.00, 40.00, 60.00, 4.80, 2.00, 5.00, 41.20, 46.00, 49.20, 54.00, 8.00),
    (5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 567890123, 5, 50.00, 75.00, 60.00, 5.00, 60.00, 50.00, 75.00, 6.00, 2.50, 6.00, 51.50, 57.50, 61.50, 67.50, 10.00),
    (6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 678901234, 6, 60.00, 90.00, 72.00, 6.00, 72.00, 60.00, 90.00, 7.20, 3.00, 7.00, 61.80, 69.00, 73.80, 81.00, 12.00),
    (7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 789012345, 7, 70.00, 105.00, 84.00, 7.00, 84.00, 70.00, 105.00, 8.40, 3.50, 8.00, 72.10, 80.50, 86.10, 93.50, 14.00),
    (8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 890123456, 8, 80.00, 120.00, 96.00, 8.00, 96.00, 80.00, 120.00, 9.60, 4.00, 9.00, 82.40, 92.00, 98.40, 106.00, 16.00),
    (9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 901234567, 9, 90.00, 135.00, 108.00, 9.00, 108.00, 90.00, 135.00, 10.80, 4.50, 10.00, 92.70, 103.50, 110.70, 118.50, 18.00),
    (10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 1012345678, 10, 100.00, 150.00, 120.00, 10.00, 120.00, 100.00, 150.00, 12.00, 5.00, 11.00, 103.00, 115.00, 123.00, 131.00, 20.00),
    (11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 1123456789, 11, 110.00, 165.00, 132.00, 11.00, 132.00, 110.00, 165.00, 13.20, 5.50, 12.00, 113.30, 126.50, 133.30, 143.50, 22.00),
    (12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 1234567890, 12, 120.00, 180.00, 144.00, 12.00, 144.00, 120.00, 180.00, 14.40, 6.00, 13.00, 123.60, 138.00, 143.60, 155.00, 24.00),
    (13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 1345678901, 13, 130.00, 195.00, 156.00, 13.00, 156.00, 130.00, 195.00, 15.60, 6.50, 14.00, 133.90, 149.50, 153.90, 166.50, 26.00),
    (14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 1456789012, 14, 140.00, 210.00, 168.00, 14.00, 168.00, 140.00, 210.00, 16.80, 7.00, 15.00, 144.20, 161.00, 164.20, 178.00, 28.00),
    (15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 1567890123, 15, 150.00, 225.00, 180.00, 15.00, 180.00, 150.00, 225.00, 18.00, 7.50, 16.00, 154.50, 172.50, 174.50, 189.50, 30.00),
    (16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 1678901234, 16, 160.00, 240.00, 192.00, 16.00, 192.00, 160.00, 240.00, 19.20, 8.00, 17.00, 164.80, 184.00, 184.80, 201.00, 32.00),
    (17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 1789012345, 17, 170.00, 255.00, 204.00, 17.00, 204.00, 170.00, 255.00, 20.40, 8.50, 18.00, 175.10, 195.50, 195.10, 212.50, 34.00),
    (18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 1890123456, 18, 180.00, 270.00, 216.00, 18.00, 216.00, 180.00, 270.00, 21.60, 9.00, 19.00, 185.40, 207.00, 205.40, 224.00, 36.00),
    (19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 1991234567, 19, 190.00, 285.00, 228.00, 19.00, 228.00, 190.00, 285.00, 22.80, 9.50, 20.00, 195.70, 218.50, 215.70, 235.50, 38.00),
    (20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 2092345678, 20, 200.00, 300.00, 240.00, 20.00, 240.00, 200.00, 300.00, 24.00, 10.00, 21.00, 206.00, 230.00, 226.00, 247.00, 40.00)
;