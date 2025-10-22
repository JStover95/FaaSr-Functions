#!/bin/bash

IMAGE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--image)
            IMAGE=$2
            shift 2
            ;;
        *)
            echo "Invalid argument: $1"
            exit 1
            ;;
    esac
done

if [ -z "$IMAGE" ]; then
    IMAGES=$(ls *.png)

    for IMAGE in $IMAGES; do
        magick $IMAGE -resize 600x600 $(basename $IMAGE .png)-600px.png
    done

    exit 0
fi

magick $IMAGE -resize 600x600 $(basename $IMAGE .png)-600px.png
