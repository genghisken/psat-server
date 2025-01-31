
export PYTHONVRA=/usr/local/swtools/python/atls/anaconda3/envs/vra/bin/python
export PYTHONPATH=/usr/local/ps1code/gitrelease/atlasvras:/usr/local/ps1code/gitrelease/atlasapiclient

export SLACK_TOKEN=`cat /usr/local/ps1code/gitrelease/psat-server/psat-server/scripts/utils/bash/vra_slack_token.txt`

$PYTHONVRA /usr/local/ps1code/gitrelease/atlasvras/atlasvras/st3ph3n/slackbot.py
