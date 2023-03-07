--
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  user_2
-- ----------------------------------------------------------------------------------------------------------------

-- Create user_table_2_1 table
CREATE TABLE user_table_2_1 (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL DEFAULT 'flink',
    address VARCHAR(1024),
    phone_number VARCHAR(512)
);

-- Create user_table_2_2 table
CREATE TABLE user_table_2_2 (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL DEFAULT 'flink',
    address VARCHAR(1024),
    phone_number VARCHAR(512),
    age INTEGER
);

INSERT INTO user_table_2_1
VALUES (211,"user_211","Shanghai","123567891234");

INSERT INTO user_table_2_2
VALUES (221,"user_221","Shanghai","123567891234", 18);