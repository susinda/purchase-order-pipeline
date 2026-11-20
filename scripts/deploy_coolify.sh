#!/usr/bin/env bash
set -euo pipefail

: "${COOLIFY_API_URL:?Set COOLIFY_API_URL to the Coolify API base URL ending in /api/v1}"
: "${COOLIFY_SERVICE_UUID:?Set COOLIFY_SERVICE_UUID to the Compose Service UUID}"
: "${COOLIFY_AUTH_TOKEN:?Set the COOLIFY_AUTH_TOKEN GitHub Actions secret}"

api_url=${COOLIFY_API_URL%/}
curl_coolify() {
    curl --proto '=https' --fail --silent --show-error --max-time 15 \
        --header "Authorization: Bearer $COOLIFY_AUTH_TOKEN" \
        --header "Accept: application/json" \
        --header "Content-Type: application/json" "$@"
}

case "${1:-}" in
    update-tag)
        : "${GITHUB_SHA:?GITHUB_SHA must contain the published image tag}"
        tag_update=$(jq -cn --arg tag "$GITHUB_SHA" '{key: "IMAGE_TAG", value: $tag}')
        curl_coolify --request PATCH --data "$tag_update" \
            "$api_url/services/$COOLIFY_SERVICE_UUID/envs" > /dev/null
        printf 'Updated Coolify IMAGE_TAG to %s\n' "$GITHUB_SHA"
        ;;
    deploy)
        # Coolify returns a service message rather than an application deployment UUID.
        message=$(
            curl_coolify --request POST "$api_url/deploy?uuid=$COOLIFY_SERVICE_UUID&force=false" |
                jq -er --arg uuid "$COOLIFY_SERVICE_UUID" '
                    .deployments[] | select(.resource_uuid == $uuid) | .message | strings'
        )
        if [[ "$message" != "Service "* ]]; then
            printf 'Coolify did not accept deployment: %s\n' "$message" >&2
            exit 1
        fi
        printf '%s\n' "$message"
        ;;
    *)
        printf 'Usage: bash %s {update-tag|deploy}\n' "$0" >&2
        exit 2
        ;;
esac
