set SJS_VERSION=%1

setlocal
cd %~dp0

set PYTHON_EXECUTABLE=%2

%PYTHON_EXECUTABLE% %~dp0%3 build --build-base ../../target/python^
 egg_info --egg-base ../../target/python^
 bdist_egg --bdist-dir /tmp/bdist --dist-dir ../../target/python --skip-build

%PYTHON_EXECUTABLE% %~dp0%3 build --build-base ../../target/python^
 bdist_wheel --bdist-dir /tmp/bdist --dist-dir ../../target/python --skip-build

endlocal