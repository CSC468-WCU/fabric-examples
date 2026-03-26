FROM python:3.11.15-trixie

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=/home/fabric/.local/bin:$PATH

ARG FABRIC_PROJECT_ID
ARG FABRIC_BASTION_USERNAME

ENV FABRIC_PROJECT_ID=$FABRIC_PROJECT_ID
ENV FABRIC_BASTION_USERNAME=$FABRIC_BASTION_USERNAME

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo \
    openssh-client \
    git \
    curl \
    ca-certificates \
    bash-completion \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Create fabric user with passwordless sudo
RUN useradd -m -s /bin/bash fabric && \
    echo "fabric ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/fabric && \
    chmod 0440 /etc/sudoers.d/fabric

# Bring in docker-side templates and optional token
COPY .docker/ /tmp/docker/
COPY requirements.txt /tmp/requirements.txt

RUN mkdir -p /home/fabric/.ssh /home/fabric/.fabric /app && \
    chown -R fabric:fabric /home/fabric /app /tmp/requirements.txt /tmp/docker

USER fabric
WORKDIR /app

# Install Jupyter and project requirements as the fabric user
RUN python -m pip install --user --upgrade pip && \
    python -m pip install --user jupyter jupyterlab && \
    if [ -f /tmp/requirements.txt ]; then python -m pip install --user -r /tmp/requirements.txt; fi

# Generate SSH keys and build FABRIC config files
RUN mkdir -p "$HOME/.ssh" "$HOME/.fabric" && \
    chmod 700 "$HOME/.ssh" "$HOME/.fabric" && \
    if [ ! -f "$HOME/.ssh/slice_key" ]; then \
      ssh-keygen -q -t rsa -b 4096 -N "" -f "$HOME/.ssh/slice_key"; \
    fi && \
    sed \
      -e "s|__FABRIC_PROJECT_ID__|${FABRIC_PROJECT_ID}|g" \
      -e "s|__FABRIC_BASTION_USERNAME__|${FABRIC_BASTION_USERNAME}|g" \
      /tmp/docker/fabric_env.sh.template > "$HOME/fabric_env.sh" && \
    cp "$HOME/fabric_env.sh" "$HOME/.fabric/fabric_rc" && \
    chmod 600 "$HOME/fabric_env.sh" "$HOME/.fabric/fabric_rc" && \
    chmod 600 "$HOME/.ssh/slice_key" && \
    chmod 644 "$HOME/.ssh/slice_key.pub" && \
    echo 'if [ -f "$HOME/fabric_env.sh" ]; then . "$HOME/fabric_env.sh"; fi' >> "$HOME/.bashrc"

EXPOSE 8888 3000 4000 8080

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["bash", "-lc", ". \"$HOME/fabric_env.sh\" && cd /app && exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token='' --ServerApp.password=''"]