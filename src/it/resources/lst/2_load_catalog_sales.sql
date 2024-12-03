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

INSERT INTO "PUBLIC"."catalog_sales" (
    cs_sold_time_sk,
    cs_ship_date_sk,
    cs_bill_customer_sk,
    cs_bill_cdemo_sk,
    cs_bill_hdemo_sk,
    cs_bill_addr_sk,
    cs_ship_customer_sk,
    cs_ship_cdemo_sk,
    cs_ship_hdemo_sk,
    cs_ship_addr_sk,
    cs_call_center_sk,
    cs_catalog_page_sk,
    cs_ship_mode_sk,
    cs_warehouse_sk,
    cs_item_sk,
    cs_promo_sk,
    cs_order_number,
    cs_quantity,
    cs_wholesale_cost,
    cs_list_price,
    cs_sales_price,
    cs_ext_discount_amt,
    cs_ext_sales_price,
    cs_ext_wholesale_cost,
    cs_ext_list_price,
    cs_ext_tax,
    cs_coupon_amt,
    cs_ext_ship_cost,
    cs_net_paid,
    cs_net_paid_inc_tax,
    cs_net_paid_inc_ship,
    cs_net_paid_inc_ship_tax,
    cs_net_profit,
    cs_sold_date_sk
) VALUES
(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 1, 20, 100.00, 150.00, 120.00, 10.00, 120.00, 100.00, 150.00, 5.00, 2.00, 10.00, 115.00, 120.00, 125.00, 130.00, 95.00, 1),
(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 2, 25, 110.00, 160.00, 130.00, 12.00, 130.00, 110.00, 160.00, 6.00, 2.50, 11.00, 120.00, 125.00, 130.00, 135.00, 100.00, 2),
(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 3, 30, 120.00, 170.00, 140.00, 15.00, 140.00, 120.00, 170.00, 7.00, 3.00, 12.00, 130.00, 135.00, 140.00, 145.00, 105.00, 3),
(4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 4, 35, 130.00, 180.00, 150.00, 18.00, 150.00, 130.00, 180.00, 8.00, 3.50, 13.00, 140.00, 145.00, 150.00, 155.00, 110.00, 4),
(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 5, 40, 140.00, 190.00, 160.00, 20.00, 160.00, 140.00, 190.00, 9.00, 4.00, 14.00, 150.00, 155.00, 160.00, 165.00, 115.00, 5),
(6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 6, 45, 150.00, 200.00, 170.00, 22.00, 170.00, 150.00, 200.00, 10.00, 4.50, 15.00, 160.00, 165.00, 170.00, 175.00, 120.00, 6),
(7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 7, 50, 160.00, 210.00, 180.00, 25.00, 180.00, 160.00, 210.00, 11.00, 5.00, 16.00, 170.00, 175.00, 180.00, 185.00, 125.00, 7),
(8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 8, 55, 170.00, 220.00, 190.00, 28.00, 190.00, 170.00, 220.00, 12.00, 5.50, 17.00, 180.00, 185.00, 190.00, 195.00, 130.00, 8),
(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 9, 60, 180.00, 230.00, 200.00, 30.00, 200.00, 180.00, 230.00, 13.00, 6.00, 18.00, 190.00, 195.00, 200.00, 205.00, 135.00, 9),
(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 10, 65, 190.00, 240.00, 210.00, 32.00, 210.00, 190.00, 240.00, 14.00, 6.50, 19.00, 200.00, 205.00, 210.00, 215.00, 140.00, 10),
(11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 123456789012355, 70, 200.00, 250.00, 220.00, 35.00, 220.00, 200.00, 250.00, 15.00, 7.00, 20.00, 210.00, 215.00, 220.00, 225.00, 145.00, 20230829),
(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 123456789012356, 75, 210.00, 260.00, 230.00, 37.00, 230.00, 210.00, 260.00, 16.00, 7.50, 21.00, 220.00, 225.00, 230.00, 235.00, 150.00, 20230830),
(13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 123456789012357, 80, 220.00, 270.00, 240.00, 40.00, 240.00, 220.00, 270.00, 17.00, 8.00, 22.00, 230.00, 235.00, 240.00, 245.00, 155.00, 20230831),
(14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 123456789012358, 85, 230.00, 280.00, 250.00, 42.00, 250.00, 230.00, 280.00, 18.00, 8.50, 23.00, 240.00, 245.00, 250.00, 255.00, 160.00, 20230901),
(15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 123456789012359, 90, 240.00, 290.00, 260.00, 45.00, 260.00, 240.00, 290.00, 19.00, 9.00, 24.00, 250.00, 255.00, 260.00, 265.00, 165.00, 20230902),
(16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 123456789012360, 95, 250.00, 300.00, 270.00, 47.00, 270.00, 250.00, 300.00, 20.00, 9.50, 25.00, 260.00, 265.00, 270.00, 275.00, 170.00, 20230903),
(17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 123456789012361, 100, 260.00, 310.00, 280.00, 50.00, 280.00, 260.00, 310.00, 21.00, 10.00, 26.00, 270.00, 275.00, 280.00, 285.00, 175.00, 20230904),
(18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 123456789012362, 105, 270.00, 320.00, 290.00, 52.00, 290.00, 270.00, 320.00, 22.00, 10.50, 27.00, 280.00, 285.00, 290.00, 295.00, 180.00, 20230905),
(19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 123456789012363, 110, 280.00, 330.00, 300.00, 55.00, 300.00, 280.00, 330.00, 23.00, 11.00, 28.00, 290.00, 295.00, 300.00, 305.00, 185.00, 20230906),
(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 123456789012364, 115, 290.00, 340.00, 310.00, 58.00, 310.00, 290.00, 340.00, 24.00, 11.50, 29.00, 300.00, 305.00, 310.00, 315.00, 190.00, 20230907);