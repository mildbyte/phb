# phb

* `poetry install`
* `cp config.yml.example config.yml`
* Create a Mattermost incoming hook pointing to the bot (so that it can receive @phb commands), add token to `mattermost_token` in config.yml
* Create a Mattermost outgoing hook (so that the bot can nag you to send updates -- not implemented yet), add hook URL to `mattremost_webhook` in config.yml
* `poetry run python phb.py`
