#!/bin/sh
set -eu

swift build -c release
codesign --force --options runtime --entitlements Resources/ChevalierVZ.entitlements --sign "${CHEVALIER_VZ_CODESIGN_IDENTITY:--}" .build/release/chevalier-vz
codesign --verify --strict --verbose=2 .build/release/chevalier-vz
