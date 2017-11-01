#!/bin/bash

EXPORTER_URL=$1
AUTH=$2
UUID_LIST=$3

if [ -z "${EXPORTER_URL}" ]; then
  echo ">>Exporter URL is empty but is mandatory in the form of \"https://your-url.ft.com/__content-export\""
  exit 1
fi
if [ -z "${AUTH}" ]; then
  echo ">>Authentication is empty but is mandatory in the form of \"Basic xxx\""
  exit 1
fi
postBody=""
if [ -n "${CONCEPT_TYPES}" ]; then
  echo "Export will be made for the following concept types: ${CONCEPT_TYPES}"
  postBody="{\"conceptTypes\":\"${CONCEPT_TYPES}\"}"
else
  echo "FULL concept export initiated."
fi
jobResult=`curl -qSfs "${EXPORTER_URL}/export" -H "Authorization: ${AUTH}" -XPOST -d "${postBody}" 2>/dev/null`
if [ "$?" -ne 0 ]; then
  echo ">>Exporter service cannot be called successfully. Maybe service is down or the authentication is incorrect or there is already a running export job? Or no valid candidate concept types in the request?"
  exit 1
else
  jobID=`echo "${jobResult}" | jq '.ID' | cut -d'"' -f2 2>/dev/null`
  echo "Export triggered. Job id: ${jobID}. Checking continuously the status to be in 'Finished'..."
  sleep 3
  status="Running"
  while [ ${status} != "Finished" ]; do
  job=`curl -qSfs "${EXPORTER_URL}/job" -H "Authorization: ${AUTH}" 2>/dev/null`

  if [ "$?" -ne 0 ]; then
	echo ">>Failed to retrieve job"
	exit 1
  else
	 status=`echo ${job} | jq '.Status' | cut -d'"' -f2 2>/dev/null`
  fi

  echo ${job}
  sleep 3
  done
  echo "Export finished. Check logs if there are failures"
fi
