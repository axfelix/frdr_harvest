#!/bin/bash

find ./ -type f -exec sed -i -e 's/%s/?/g' {} \; && find ./ -type f -exec sed -i -e '/log/ s/?/%s/' {} \; && find ./ -type f -exec sed -i -e '/log/ s/?/%s/' {} \; && find ./ -type f -exec sed -i -e '/log/ s/?/%s/' {} \;