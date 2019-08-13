#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset

PORT=8000

# Wait for the app to be started and listening
while ! nc -z -w 30 app $PORT; do
  if [ "${i:=0}" -ge 3 ]; then
    echo Timeout waiting for app >&2
    exit 1
  fi
  i=$(($i + 1))
  sleep 5
done

# Call GetCapabilities, look for the ProviderName
curl -v --get --fail --silent "http://app:$PORT" \
  --data-urlencode service=CSW \
  --data-urlencode version=2.0.2 \
  --data-urlencode request=GetCapabilities | \
  grep --quiet '<ows:ProviderName>General Services Administration</ows:ProviderName>'
