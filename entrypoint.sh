#!/bin/sh
   node /root/bgutil-ytdlp-pot-provider/server/build/main.js &
   exec uvicorn main:app --host 0.0.0.0 --port $PORT