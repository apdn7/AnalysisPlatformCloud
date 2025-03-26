# To build apdn7/analysisplatform image in docker
# Run below command to build with version you want
#    docker build --pull --rm -f "Dockerfile" --no-cache --tag "apdn7/analysisplatform:latest" --label "apdn7/analysisplatform"
#
# Export built image to file
#    docker save -o ./apdn7_analysisplatform.tar  apdn7/analysisplatform
#
# Import a built image file into docker
#    docker image load < apdn7_analysisplatform.tar
#
# Sometime you need to remove cache befor build image by using this command
#    docker builder prune -a

FROM python:3.9-slim-bookworm

ARG BASE_DIR=/app
ARG APP_WORKDIR=$BASE_DIR/analysisinterface

RUN mkdir -p $APP_WORKDIR
COPY . $APP_WORKDIR

# Set default working directory
WORKDIR $APP_WORKDIR

# Expose the application port
EXPOSE 7770

# Define the command to run your application
CMD ["bash", "start_ap.bash"]
