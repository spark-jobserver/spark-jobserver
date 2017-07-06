set SJS_VERSION=%1

setlocal
cd %~dp0

python %~dp0%2 build --build-base ../../target/python^
 egg_info --egg-base ../../target/python^
 bdist_egg --bdist-dir /tmp/bdist --dist-dir ../../target/python --skip-build

endlocal