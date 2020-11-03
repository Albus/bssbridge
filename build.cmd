pushd
del /F /S /Q nuitka-build
nuitka --standalone --windows-dependency-tool=pefile ^
    --experimental=use_pefile_recurse --experimental=use_pefile_fullrecurse ^
    --include-module=sentry_sdk ^
    --show-progress --show-modules --remove-output --output-dir=nuitka-build -j 3 --clang bssbridge
popd
rename nuitka-build\bssbridge.dist\bssbridge.exe bb.exe