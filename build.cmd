del /F /S /Q nuitka-build
nuitka --standalone --windows-dependency-tool=pefile --experimental=use_pefile_recurse --show-progress ^
--include-module=sentry_sdk ^
--show-modules --remove-output --output-dir=nuitka-build -j 3 --clang bssbridge