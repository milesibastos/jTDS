@echo off
set JAVA_INCLUDES=;%JAVA_HOME%\include;%JAVA_HOME%\include\win32
set INCLUDE=%INCLUDE%;%JAVA_INCLUDES%
if exist ".\Release/" del /Q .\Release\*.*
NMAKE /f "ntlmauth.mak" CFG="ntlmauth - Win32 Release"

