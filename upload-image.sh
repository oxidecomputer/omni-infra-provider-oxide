#!/bin/bash

curl -L -o /tmp/image.raw.xz "${1}"

xz -d /tmp/image.raw.xz

oxide disk import \
    --project matthewsanabria \
    --path /tmp/image.raw \
    --disk talos-infra-provider \
    --description "Talos Linux." \
    --snapshot talos-infra-provider \
    --image ${2} \
    --image-description "Talos Linux" \
    --image-os "Talos Linux" \
    --image-version "v1.11.2"

oxide snapshot delete \
    --project matthewsanabria \
    --snapshot talos-infra-provider

oxide disk delete \
    --project matthewsanabria \
    --disk talos-infra-provider

rm /tmp/image.raw
