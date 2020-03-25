#!/bin/sh

# Decrypt the file
# mkdir $HOME/secrets
# --batch to prevent interactive command --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
--output snowflake.travis.json .github/scripts/snowflake.travis.json.gpg
