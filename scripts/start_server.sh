service docker start
curl -L https://github.com/docker/compose/releases/download/1.20.0/docker-compose-`uname -s`-`uname -m` -o /opt/bin/docker-compose
chmod +x /opt/bin/docker-compose
/opt/bin/docker-compose up build
/opt/bin/docker-compose up -d > /dev/null 2> /dev/null < /dev/null &
