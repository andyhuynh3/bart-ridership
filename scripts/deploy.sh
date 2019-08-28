

REGISTRY_URL=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
LATEST_APP_COMMIT=$(git log -n 1 --pretty=format:%H app)
LATEST_NGINX_COMMIT=$(git log -n 1 --pretty=format:%H app)
LATEST_DOCKERFILE_COMMIT=$(git log -n 1 --pretty=format:%H app)

if [ $TRAVIS_COMMIT = $LATEST_APP_COMMIT -o $TRAVIS_COMMIT = $LATEST_NGINX_COMMIT ]; then
    $(aws ecr get-login --no-include-email)
    if [ $TRAVIS_COMMIT = $LATEST_APP_COMMIT ]; then
        echo 'Found changes to app, kicking off Docker build, tag, push'
        SOURCE_WEB_IMAGE=bart_web:latest
        TARGET_WEB_IMAGE=$REGISTRY_URL/$SOURCE_WEB_IMAGE
        docker build ./app -t $SOURCE_WEB_IMAGE
        if [ $? -eq 0 ]; then
            docker tag $SOURCE_WEB_IMAGE $TARGET_WEB_IMAGE
            docker push $TARGET_WEB_IMAGE
        else
            echo 'Building app image failed'
            exit 1
        fi
    fi
    if [ $TRAVIS_COMMIT = $LATEST_NGINX_COMMIT ]; then
        echo 'Found changes to nginx, kicking off Docker build, tag, push'
        SOURCE_NGINX_IMAGE=bart_nginx:latest
        TARGET_NGINX_IMAGE=$REGISTRY_URL/$SOURCE_NGINX_IMAGE
        docker build ./nginx -t $SOURCE_NGINX_IMAGE
        if [ $? -eq 0 ]; then
            docker tag $SOURCE_NGINX_IMAGE $TARGET_NGINX_IMAGE
            docker push $TARGET_NGINX_IMAGE
        else
            echo 'Building nginx image failed'
            exit 1
        fi
    fi
     ecs deploy $CLUSTER_NAME $SERVICE_NAME
    exit 0
else
    echo 'No changes detected, no need to kick off docker processes'
    exit 0
fi
