# syntax=docker/dockerfile:1.4
ARG USERNAME=vscode

FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm

ARG USERNAME

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install mold

# Run the rest of the commands as the non-root user
USER $USERNAME

# Set up a config.toml file that makes Cargo use clang and mold
RUN mkdir -p /home/${USERNAME}/.cargo \
    && echo "[profile.debug.target.x86_64-unknown-linux-gnu]" >> /home/${USERNAME}/.cargo/config.toml \
    && echo "linker = \"clang\"" >> /home/${USERNAME}/.cargo/config.toml \
    && echo "rustflags = [\"-C\", \"link-arg=-fuse-ld=/usr/local/bin/mold\"]" >> /home/${USERNAME}/.cargo/config.toml \
    && echo "[registries.crates-io]" >> /home/${USERNAME}/.cargo/config.toml \
    && echo "protocol = \"sparse\"" >> /home/${USERNAME}/.cargo/config.toml \
    && echo "[net]" >> /home/${USERNAME}/.cargo/config.toml \
    && echo "git-fetch-with-cli = true" >> /home/${USERNAME}/.cargo/config.toml

RUN cargo install cargo-machete
