#!/usr/bin/env bash

git tag "release-$(date "+%Y-%m-%d")"
git push origin --tags