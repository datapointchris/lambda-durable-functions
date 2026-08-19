#!/usr/bin/env bash
# Update a durable function's code and, optionally, make the alias point at it.
#
# A durable function must be invoked through a qualified ARN, so the trigger
# follows an alias and the alias points at an immutable version. Updating code
# alone leaves the trigger running the old version.
set -euo pipefail

FUNCTION_NAME="${FUNCTION_NAME:-error-notifier}"
ALIAS_NAME="${ALIAS_NAME:-live}"
ARTIFACT="${ARTIFACT:-function.zip}"

usage() {
    cat <<'EOF'
Usage: deploy.sh <draft|release>

  draft    update $LATEST only. Invoke $LATEST directly to iterate; the
           trigger keeps running whatever the alias points at.
  release  publish a new version and move the alias onto it, so the
           trigger sees the change.

Terraform owns the real release path via publish = true and
source_code_hash. Direct deploys create drift that the next apply
reconciles, so use them for iteration only.

Environment: FUNCTION_NAME, ALIAS_NAME, ARTIFACT
EOF
}

require_artifact() {
    [[ -f "$ARTIFACT" ]] || {
        echo "artifact not found: $ARTIFACT" >&2
        exit 1
    }
}

case "${1:-}" in
draft)
    require_artifact
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file "fileb://$ARTIFACT" \
        --query 'LastModified' --output text
    ;;
release)
    require_artifact
    version="$(aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file "fileb://$ARTIFACT" \
        --publish --query Version --output text)"
    aws lambda update-alias \
        --function-name "$FUNCTION_NAME" \
        --name "$ALIAS_NAME" \
        --function-version "$version" \
        --query 'AliasArn' --output text
    echo "$ALIAS_NAME -> version $version"
    ;;
*)
    usage
    exit 1
    ;;
esac
