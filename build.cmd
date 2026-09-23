@echo off
rem
rem Licensed to the Apache Software Foundation (ASF) under one
rem or more contributor license agreements.  See the NOTICE file
rem distributed with this work for additional information
rem regarding copyright ownership.  The ASF licenses this file
rem to you under the Apache License, Version 2.0 (the
rem "License"); you may not use this file except in compliance
rem with the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem
rem Build this project against a chosen Flink version line.
rem
rem Usage:
rem   build.cmd 1.20        Flink 1.20.0 + Flink CDC 3.6.0-1.20                  [default]
rem   build.cmd 2.2         Flink 2.2.0  + Flink CDC 3.6.0-2.2
rem   build.cmd 1.20 test   run `test` instead of the default `verify`
rem   build.cmd all         build both lines to prove the tree is version-agnostic
rem
rem Both lines use Flink CDC 3.x (package org.apache.flink.cdc.*), so there is no CDC compatibility
rem shim; only dependency coordinates differ.
rem
rem Flag meanings:
rem   -Pflink-1.20 / -Pflink-2.2   switch Flink, CDC and JDBC connector versions
rem   -Pflink20 / -Pflink2         select the version-specific source set in flink-compat
rem   -Pflink2-test-guava          work around upstream FLINK-39429 (Flink 2.x only)
rem
rem The source-set flag is needed separately from the version flag because Maven cannot switch a
rem child module's source roots from the parent, and profile activation does not cascade.

setlocal
set LINE=%1
if "%LINE%"=="" set LINE=1.20
set GOAL=%2
if "%GOAL%"=="" set GOAL=verify

if /I "%LINE%"=="1.20" (
  set PROFILES=-Pflink-1.20
  goto run
)
if /I "%LINE%"=="2.2" (
  rem flink2-test-guava appends the CDC connector's guava31 shade to the test classpath; without it
  rem the end-to-end test dies with NoClassDefFoundError on Flink 2.2. See FLINK-39429.
  set PROFILES=-Pflink-2.2 -Pflink2 -Pflink2-test-guava
  goto run
)
if /I "%LINE%"=="all" (
  echo ==^> Building Flink 1.20.0 / CDC 3.6.0-1.20
  call mvnw.cmd -Pflink-1.20 %GOAL%
  if errorlevel 1 exit /b 1
  echo ==^> Building Flink 2.2.0 / CDC 3.6.0-2.2
  call mvnw.cmd -Pflink-2.2 -Pflink2 -Pflink2-test-guava %GOAL%
  if errorlevel 1 exit /b 1
  echo ==^> Both version lines built successfully.
  exit /b 0
)
echo Unknown version line: %LINE%
echo Expected one of: 1.20 ^| 2.2 ^| all
exit /b 2

:run
echo ==^> mvnw %PROFILES% %GOAL%
call mvnw.cmd %PROFILES% %GOAL%
exit /b %errorlevel%
